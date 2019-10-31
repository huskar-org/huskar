from __future__ import absolute_import
import logging

from sqlalchemy import Column, Integer, Unicode, UniqueConstraint
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.exc import IntegrityError

# TODO Do not use this base exception in future
from huskar_api.service.exc import HuskarApiException
from huskar_api.models.db import UpsertMixin
from huskar_api.models.signals import (
    team_will_be_archived, team_will_be_deleted)
from huskar_api.models import (
    DeclarativeBase, CacheMixin, DBSession, cache_on_arguments)
from huskar_api.models.exceptions import NameOccupiedError
from .user import User
from .role import Authority

logger = logging.getLogger(__name__)


class Team(CacheMixin, DeclarativeBase):
    """The team which organized applications of Huskar."""

    __tablename__ = 'team'

    #: The team name which serves minimal mode
    DEFAULT_NAME = 'default'

    STATUS_ACTIVE = 0
    STATUS_ARCHIVED = 1

    id = Column(Integer, primary_key=True)
    team_name = Column(Unicode(32, collation='utf8mb4_bin'),
                       nullable=False, unique=True)
    team_desc = Column(Unicode(128, collation='utf8mb4_bin'))
    status = Column(TINYINT, nullable=False, default=STATUS_ACTIVE)

    @classmethod
    def get_by_name(cls, name):
        team_id = cls.get_id_by_name(name)
        if team_id is not None:
            return cls.get(team_id)

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_id_by_name(cls, name):
        cond = (
            (cls.team_name == name) &
            (cls.status == cls.STATUS_ACTIVE)
        )
        return DBSession().query(cls.id).filter(cond).scalar()

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_all_ids(cls):
        rs = DBSession().query(cls.id) \
                        .filter_by(status=cls.STATUS_ACTIVE).all()
        return sorted(r[0] for r in rs)

    @classmethod
    def get_multi_by_admin(cls, user_id):
        ids = TeamAdmin.get_team_ids(user_id)
        return cls.mget(ids)

    @classmethod
    def get_all(cls):
        ids = cls.get_all_ids()
        return cls.mget(ids)

    @classmethod
    def create(cls, name, desc=None):
        """Creates a new team.

        :param name: The unique name of team.
        :param desc: The readable name of team.
        :returns: The instance of :class:`Team`.
        """
        try:
            with DBSession().close_on_exit(False) as db:
                instance = cls(team_name=name, team_desc=desc or name)
                db.add(instance)
        except IntegrityError:
            raise NameOccupiedError

        cls.flush([instance.id])
        cls.get_id_by_name.flush(instance.team_name)
        cls.get_all_ids.flush()
        return instance

    def _set_status(self, status):
        with DBSession().close_on_exit(False):
            self.status = status
        self.__class__.flush([self.id])
        self.__class__.get_all_ids.flush()
        self.__class__.get_id_by_name.flush(self.team_name)

    @classmethod
    def delete(cls, team_id):
        team = cls.get(team_id)
        with DBSession().close_on_exit(False) as db:
            team_will_be_deleted.send(cls, db=db, team_id=team_id)
            for user_id in TeamAdmin.get_user_ids(team_id):
                TeamAdmin.discard(team_id, user_id)
            db.query(cls).filter_by(id=team_id).delete()
        if team is not None:  # can be skip safety (instance will be erased)
            cls.get_id_by_name.flush(team.team_name)
        cls.get_all_ids.flush()
        cls.flush([team_id])

    def rename_desc(self, new_desc):
        with DBSession().close_on_exit(False):
            self.team_desc = new_desc
        self.__class__.flush([self.id])

    def archive(self):
        team_will_be_archived.send(
            self.__class__, db=DBSession(), team_id=self.id)
        self._set_status(self.STATUS_ARCHIVED)

    def unarchive(self):
        self._set_status(self.STATUS_ACTIVE)

    @property
    def is_active(self):
        return self.status == self.STATUS_ACTIVE

    def check_is_admin(self, user_id):
        """Checks a user is admin of this team or not."""
        ids = TeamAdmin.get_user_ids(self.id)
        return int(user_id) in ids

    def list_admin(self):
        """Get the list of admin users for this team.

        :returns: The list of :class:`User`.
        """
        ids = TeamAdmin.get_user_ids(self.id)
        return User.mget(ids)

    def grant_admin(self, user_id):
        """Grants user as an admin for this team.

        :param user_id: The id of user.
        """
        TeamAdmin.ensure(self.id, user_id)

    def dismiss_admin(self, user_id):
        """Dismisses user's admin role for this team.

        :param user_id: The id of user.
        """
        TeamAdmin.discard(self.id, user_id)

    def check_auth(self, authority, user_id):
        assert authority in Authority
        return self.check_is_admin(user_id)


class TeamAdmin(CacheMixin, UpsertMixin, DeclarativeBase):
    __tablename__ = 'team_admin'
    __table_args__ = (
        UniqueConstraint(
            'user_id', 'team_id', name='uq_team_admin',
        ),
        DeclarativeBase.__table_args__,
    )

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, nullable=False, index=True)
    user_id = Column(Integer, nullable=False)

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_user_ids(cls, team_id):
        rs = DBSession().query(cls.user_id) \
                        .filter_by(team_id=team_id) \
                        .order_by(cls.id.asc()) \
                        .all()
        return [r[0] for r in rs]

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_team_ids(cls, user_id):
        rs = DBSession().query(cls.team_id) \
                        .filter_by(user_id=user_id) \
                        .order_by(cls.id.asc()) \
                        .all()
        return [r[0] for r in rs]

    @classmethod
    def flush_by(cls, team_id, user_id):
        cls.get_user_ids.flush(team_id)
        cls.get_team_ids.flush(user_id)

    @classmethod
    def ensure(cls, team_id, user_id):
        stmt = cls.upsert().values(team_id=team_id, user_id=user_id)
        with DBSession().close_on_exit(False) as db:
            db.execute(stmt)
            cls.flush_by(team_id, user_id)

    @classmethod
    def discard(cls, team_id, user_id):
        stmt = cls.__table__.delete().where(
            (cls.team_id == team_id) & (cls.user_id == user_id))
        with DBSession().close_on_exit(False) as db:
            db.execute(stmt)
            cls.flush_by(team_id, user_id)


class TeamNotEmptyError(HuskarApiException):
    pass
