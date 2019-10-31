from __future__ import absolute_import

import time
import hashlib

from sqlalchemy import Column, BigInteger, Unicode, Binary, Index
from sqlalchemy.dialects.mysql import TINYINT

from huskar_api.models import (
    DBSession, DeclarativeBase, TimestampMixin, CacheMixin, cache_on_arguments)
from huskar_api.models.db import UpsertMixin
from huskar_api.models.const import INFRA_CONFIG_KEYS


__all__ = ['InfraDownstream']


class InfraDownstream(CacheMixin, TimestampMixin, UpsertMixin,
                      DeclarativeBase):
    """The downstream of infra config."""

    __tablename__ = 'infra_downstream'
    __table_args__ = (
        Index('ix_infra_downstream_user_hash_bytes', 'user_hash_bytes',
              unique=True, mysql_length=32),
        DeclarativeBase.__table_args__,
    )

    _SCOPE_IDCS = 1
    _SCOPE_CLUSTERS = 2
    SCOPE_TYPE_CHOICES = {
        'idcs': _SCOPE_IDCS,
        'clusters': _SCOPE_CLUSTERS,
    }
    SCOPE_TYPE_NAMES = {
        _SCOPE_IDCS: 'idcs',
        _SCOPE_CLUSTERS: 'clusters',
    }

    id = Column(BigInteger, primary_key=True)
    application_name = Column(Unicode(128, collation='utf8mb4_bin'),
                              nullable=False, index=True)
    user_hash_bytes = Column(Binary(32), nullable=False, unique=False)
    user_application_name = Column(Unicode(128, collation='utf8mb4_bin'),
                                   nullable=False)
    user_infra_type = Column(Unicode(20, collation='utf8mb4_bin'),
                             nullable=False)
    user_infra_name = Column(Unicode(128, collation='utf8mb4_bin'),
                             nullable=False)
    user_scope_type = Column(TINYINT, nullable=False)
    user_scope_name = Column(Unicode(36, collation='utf8mb4_bin'),
                             nullable=False)
    user_field_name = Column(Unicode(20, collation='utf8mb4_bin'),
                             nullable=False)
    version = Column(BigInteger, nullable=False, index=True)

    @property
    def user_scope_pair(self):
        user_scope_type = self.SCOPE_TYPE_NAMES[self.user_scope_type]
        return (user_scope_type, self.user_scope_name)

    @classmethod
    def bindmany(cls):
        return Builder(cls)

    @classmethod
    def bind(cls, *args):
        return cls.bindmany().bind(*args).commit()

    @classmethod
    def unbind(cls, *args):
        return cls.bindmany().unbind(*args).commit()

    @classmethod
    def get_multi_by_application(cls, application_name):
        ids = cls.get_ids_by_application(application_name)
        return cls.mget(ids)

    @classmethod
    def flush_cache_by_application(cls, application_name):
        cls.get_ids_by_application.flush(application_name)

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_ids_by_application(cls, application_name):
        rs = DBSession().query(cls.id) \
                        .filter_by(application_name=application_name) \
                        .order_by(cls.id.asc()) \
                        .all()
        return sorted(r[0] for r in rs)


class Builder(object):
    def __init__(self, cls):
        self._cls = cls
        self._stmts = []
        self._timestamp = int(time.time() * 1000)

    def _hash(self, user_application_name, user_infra_type, user_infra_name,
              user_scope_type, user_scope_name, user_field_name):
        assert user_infra_type in INFRA_CONFIG_KEYS
        # any code name maybe not only use ascii
        if isinstance(user_infra_name, unicode):
            user_infra_name = user_infra_name.encode('utf-8')
        user_scope_type = self._cls.SCOPE_TYPE_NAMES[user_scope_type]
        h = hashlib.sha256()
        h.update(user_application_name)
        h.update(user_infra_type)
        h.update(user_infra_name)
        h.update(user_scope_type)
        h.update(user_scope_name)
        h.update(user_field_name)
        return h.digest()

    def bindmany(self):
        return self

    def bind(self, user_application_name, user_infra_type, user_infra_name,
             user_scope_type, user_scope_name, user_field_name,
             infra_application_name):
        assert user_infra_type in INFRA_CONFIG_KEYS
        user_scope_type = self._cls.SCOPE_TYPE_CHOICES[user_scope_type]
        user_hash_bytes = self._hash(
            user_application_name=user_application_name,
            user_infra_type=user_infra_type,
            user_infra_name=user_infra_name,
            user_scope_type=user_scope_type,
            user_scope_name=user_scope_name,
            user_field_name=user_field_name)
        stmt = self._cls.upsert().values(
            application_name=infra_application_name,
            user_hash_bytes=user_hash_bytes,
            user_application_name=user_application_name,
            user_infra_type=user_infra_type,
            user_infra_name=user_infra_name,
            user_scope_type=user_scope_type,
            user_scope_name=user_scope_name,
            user_field_name=user_field_name,
            version=self._timestamp)
        self._stmts.append(stmt)
        return self

    def unbind(self, user_application_name, user_infra_type, user_infra_name,
               user_scope_type, user_scope_name, user_field_name):
        assert user_infra_type in INFRA_CONFIG_KEYS
        user_scope_type = self._cls.SCOPE_TYPE_CHOICES[user_scope_type]
        user_hash_bytes = self._hash(
            user_application_name=user_application_name,
            user_infra_type=user_infra_type,
            user_infra_name=user_infra_name,
            user_scope_type=user_scope_type,
            user_scope_name=user_scope_name,
            user_field_name=user_field_name)
        stmt = self._cls.__table__.delete().where(
            (self._cls.user_hash_bytes == user_hash_bytes) &
            (self._cls.user_application_name == user_application_name) &
            (self._cls.user_infra_type == user_infra_type) &
            (self._cls.user_infra_name == user_infra_name) &
            (self._cls.user_scope_type == user_scope_type) &
            (self._cls.user_scope_name == user_scope_name) &
            (self._cls.user_field_name == user_field_name))
        self._stmts.append(stmt)
        return self

    def unbind_stale(self):
        stmt = self._cls.__table__.delete().where(
            self._cls.version < self._timestamp)
        self._stmts.append(stmt)
        return self

    def commit(self):
        stmts, self._stmts = self._stmts, []
        with DBSession().close_on_exit(False) as db:
            for stmt in stmts:
                db.execute(stmt)
        return self
