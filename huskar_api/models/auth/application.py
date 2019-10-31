from __future__ import absolute_import

from sqlalchemy import Column, Integer, Unicode, UniqueConstraint
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import cached_property
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN

from huskar_api.models import (
    DeclarativeBase, TimestampMixin, CacheMixin, DBSession, cache_on_arguments,
    huskar_client)
from huskar_api.models.db import UpsertMixin
from huskar_api.models.signals import (
    team_will_be_archived, team_will_be_deleted)
from huskar_api.models.exceptions import NameOccupiedError, OutOfSyncError
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.const import SCOPE_APPLICATION
from .team import Team, TeamNotEmptyError
from .user import User
from .role import Authority


class Application(CacheMixin, DeclarativeBase):
    __tablename__ = 'application'

    STATUS_ACTIVE = 0
    STATUS_ARCHIVED = 1

    SCOPE_TYPE = SCOPE_APPLICATION

    id = Column(Integer, primary_key=True)
    application_name = Column(Unicode(128, collation='utf8mb4_bin'),
                              nullable=False, unique=True)
    team_id = Column(Integer, nullable=False, index=True)
    status = Column(TINYINT, nullable=False, default=STATUS_ACTIVE)

    @cached_property
    def domain_name(self):
        return self.application_name.split('.', 1)[0]

    @classmethod
    def get_by_name(cls, name):
        application_id = cls.get_id_by_name(name)
        if application_id is not None:
            return cls.get(application_id)

    @classmethod
    def get_multi_by_team(cls, team_id):
        ids = cls.get_ids_by_team(team_id)
        instances = cls.mget(ids)
        return instances

    @classmethod
    def get_all(cls):
        ids = cls.get_all_ids()
        return cls.mget(ids)

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_id_by_name(cls, name):
        cond = (
            (cls.application_name == name) &
            (cls.status == cls.STATUS_ACTIVE)
        )
        return DBSession().query(cls.id).filter(cond).scalar()

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_ids_by_team(cls, team_id):
        rs = DBSession().query(cls.id) \
                        .filter_by(team_id=team_id, status=cls.STATUS_ACTIVE) \
                        .all()
        return sorted(r[0] for r in rs)

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_all_ids(cls):
        rs = DBSession().query(cls.id) \
                        .filter_by(status=cls.STATUS_ACTIVE).all()
        return sorted(r[0] for r in rs)

    def setup_default_zpath(self):
        im = InstanceManagement(
            huskar_client, self.application_name, SERVICE_SUBDOMAIN)
        info = im.get_service_info()
        try:
            if info.stat is None:
                info.data = {}
                info.save()
        except OutOfSyncError:
            pass

    @classmethod
    def create(cls, application_name, team_id):
        try:
            with DBSession().close_on_exit(False) as db:
                cls.check_default_user(application_name)
                instance = cls(application_name=application_name,
                               team_id=team_id)
                db.add(instance)
        except IntegrityError:
            raise NameOccupiedError
        cls.get_id_by_name.flush(application_name)
        cls.get_ids_by_team.flush(team_id)
        cls.get_all_ids.flush()
        instance.setup_default_auth()
        instance.setup_default_zpath()
        return instance

    @classmethod
    def delete(cls, application_id):
        application_id = int(application_id)
        with DBSession().close_on_exit(False) as db:
            application = cls.get(application_id)
            auth_set = ApplicationAuth.search_by(
                application_id=application_id)
            for auth in auth_set:
                db.delete(auth)
            db.flush()
            db.delete(application)
        cls.get_id_by_name.flush(application.application_name)
        cls.get_ids_by_team.flush(application.team_id)
        cls.get_all_ids.flush()
        cls.flush([application_id])

    def _set_status(self, status):
        with DBSession().close_on_exit(False):
            self.status = status
        self.flush([self.id])
        self.get_all_ids.flush()
        self.get_ids_by_team.flush(self.team.id)
        self.get_id_by_name.flush(self.application_name)

    def archive(self):
        self._set_status(self.STATUS_ARCHIVED)

    def unarchive(self):
        if self.team.is_active:
            self._set_status(self.STATUS_ACTIVE)

    @property
    def is_active(self):
        return self.status == self.STATUS_ACTIVE

    @cached_property
    def team(self):
        return Team.get(self.team_id)

    def transfer_team(self, team_id):
        orig_team_id = self.team_id
        with DBSession().close_on_exit(False):
            self.team_id = team_id
        self.get_ids_by_team.flush(team_id)
        self.get_ids_by_team.flush(orig_team_id)

    @classmethod
    def check_default_user(cls, application_name):
        user = User.get_by_name(application_name)
        if user is None or user.is_application:
            return
        raise NameOccupiedError

    def setup_default_auth(self):
        """Creates an application user and authorize it to write current
        application. It is an idempotent operation.

        :returns: The instance of application user.
        """
        # TODO record application id here
        self.check_default_user(self.application_name)
        user = User.create_application(self.application_name)
        self.ensure_auth(Authority.WRITE, user.id)
        return user

    def check_auth(self, authority, user_id):
        """Checks the authority of user for this application.

        :param authority: ``AUTHORITY_READ``, ``AUTHORITY_WRITE`` or
                          ``AUTHORITY_ADMIN``.
        :param user_id: The id of examined user.
        """
        assert authority in Authority
        return self._check_authority_with_internal_auth(authority, user_id)

    def _check_authority_with_internal_auth(self, authority, user_id):
        user = User.get(user_id)
        if user and user.is_admin:
            return True

        if self.team.check_is_admin(user_id):
            return True

        if authority == Authority.ADMIN:
            # This user don't have administration authority to current
            # application if all conditions are missed above
            return False

        auth = ApplicationAuth.find(authority, user_id, self.id)
        if auth is None and (authority == Authority.READ):
            # Because AUTHORITY_WRITE implies AUTHORITY_READ
            auth = ApplicationAuth.find(Authority.WRITE, user_id, self.id)
        return auth is not None

    def ensure_auth(self, authority, user_id):
        """Creates an application auth if it does not exist.

        :param authority: ``AUTHORITY_READ`` or ``AUTHORITY_WRITE``
        :param user_id: The id of owner.
        """
        ApplicationAuth.ensure(authority, user_id, self.id)

    def discard_auth(self, authority, user_id):
        """Deletes an application auth if it does exist.

        :param authority: ``AUTHORITY_READ`` or ``AUTHORITY_WRITE``
        :param user_id: The id of owner.
        """
        ApplicationAuth.discard(authority, user_id, self.id)

    def list_auth(self):
        """Lists all application auth, including the default auth."""
        return ApplicationAuth.search_by(application_id=self.id)


@team_will_be_archived.connect_via(Team)
def stop_team_archived(sender, db, team_id, **extra):
    cond = (
        (Application.team_id == team_id) &
        (Application.status == Application.STATUS_ACTIVE)
    )
    num = db.query(Application).filter(cond).count()
    if num == 0:
        return
    raise TeamNotEmptyError('%d application(s) in this team' % num)


@team_will_be_deleted.connect_via(Team)
def stop_team_deleting(sender, db, team_id, **extra):
    num = db.query(Application).filter_by(team_id=team_id).count()
    if num == 0:
        return
    raise TeamNotEmptyError('%d application(s) in this team' % num)


class ApplicationAuth(TimestampMixin, CacheMixin, UpsertMixin,
                      DeclarativeBase):
    __tablename__ = 'application_auth'
    __table_args__ = (
        UniqueConstraint(
            'authority', 'application_id', 'user_id', name='uq_app_auth',
        ),
        DeclarativeBase.__table_args__,
    )

    id = Column(Integer, primary_key=True)
    authority = Column(Unicode(32, collation='utf8mb4_bin'), nullable=False)
    user_id = Column(Integer, nullable=False, index=True)
    application_id = Column(Integer, nullable=False, index=True)

    @cached_property
    def user(self):
        return User.get(self.user_id)

    @classmethod
    def find(cls, authority, user_id, application_id):
        """Find an application auth by a condition-tuple.

        :param authority: ``AUTHORITY_READ`` or ``AUTHORITY_WRITE``
        :param user_id: The id of owner.
        :param application_id: The id of application.
        :returns: An :class:`ApplicationAuth` instance or ``None``.
        """
        assert authority in Authority
        id_ = cls.find_id(authority, user_id, application_id)
        return None if id_ is None else cls.get(id_)

    @classmethod
    def search_by(cls, authority=None, user_id=None, application_id=None):
        """Searches application auth by conditions.

        This method is without cache.

        :param authority: ``Authority.READ`` or ``Authority.WRITE``
        :param user_id: The id of owner.
        :param application_id: The id of application.
        :returns: A list of :class:`ApplicationAuth`.
        """
        ids = cls.search_ids_by(authority, user_id, application_id)
        return cls.mget(ids)

    @classmethod
    def search_ids_by(cls, authority, user_id, application_id):
        if authority is not None:
            assert authority in Authority
            authority = authority.value

        conditions = {name: value for name, value in [
            ('authority', authority),
            ('user_id', user_id),
            ('application_id', application_id),
        ] if value is not None}

        rs = DBSession().query(cls.id).filter_by(**conditions).order_by(
            cls.id.desc())
        return [r[0] for r in rs]

    @classmethod
    @cache_on_arguments(5 * 60)
    def find_id(cls, authority, user_id, application_id):
        ids = cls.search_ids_by(authority, user_id, application_id)
        return ids[0] if ids else None

    @classmethod
    def flush_by(cls, authority, user_id, application_id):
        ids = cls.search_ids_by(authority, user_id, application_id)
        cls.flush(ids)
        cls.find_id.flush(authority, user_id, application_id)

    @classmethod
    def ensure(cls, authority, user_id, application_id):
        """Creates an application auth if it does not exist.

        :param authority: ``AUTHORITY_READ`` or ``AUTHORITY_WRITE``
        :param user_id: The id of owner.
        :param application_id: The id of application.
        """
        assert authority in Authority

        stmt = cls.upsert().values(
            authority=authority.value, user_id=user_id,
            application_id=application_id)
        with DBSession().close_on_exit(False) as db:
            db.execute(stmt)
            cls.flush_by(authority, user_id, application_id)

    @classmethod
    def discard(cls, authority, user_id, application_id):
        """Deletes an application auth if it does exist.

        :param authority: ``AUTHORITY_READ`` or ``AUTHORITY_WRITE``
        :param user_id: The id of owner.
        :param application_id: The id of application.
        """
        assert authority in Authority
        stmt = cls.__table__.delete().where(
            (cls.authority == authority.value) & (cls.user_id == user_id) &
            (cls.application_id == application_id))
        with DBSession().close_on_exit(False) as db:
            db.execute(stmt)
            cls.flush_by(authority, user_id, application_id)
