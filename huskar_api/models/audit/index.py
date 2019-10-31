from __future__ import absolute_import

from sqlalchemy import Column, Integer, BigInteger, Index, Unicode, cast, DATE
from sqlalchemy.dialects.mysql import TINYINT

from huskar_api.models.db import UpsertMixin
from huskar_api.models import (
    DeclarativeBase, TimestampMixin, DBSession, cache_on_arguments)
from .const import (
    TYPE_SITE, TYPE_TEAM, TYPE_APPLICATION, TYPE_CONFIG, TYPE_SWITCH,
    TYPE_SERVICE)


class AuditIndex(TimestampMixin, UpsertMixin, DeclarativeBase):
    """The internal model of audit index.

    The users should never use this model directly but use the public model
    :class:`.audit.AuditLog` instead.
    """

    __tablename__ = 'audit_index'
    __table_args__ = (
        Index('ux_audit_index', 'target_id', 'target_type', 'audit_id',
              unique=True),
        DeclarativeBase.__table_args__,
    )

    TYPE_CHOICES = (TYPE_SITE, TYPE_TEAM, TYPE_APPLICATION)

    id = Column(BigInteger, primary_key=True)
    audit_id = Column(BigInteger, nullable=True)
    target_id = Column(Integer, nullable=False)
    target_type = Column(TINYINT, nullable=False)

    @classmethod
    def create(cls, db, audit_id, created_at, target_type, target_id):
        assert target_type in cls.TYPE_CHOICES
        assert not (target_type == TYPE_SITE and target_id != 0)
        stmt = cls.upsert().values(
            audit_id=audit_id, target_id=target_id, target_type=target_type,
            created_at=created_at)
        db.execute(stmt)

    @classmethod
    def flush_cache(cls, date, target_type, target_id):
        cls.get_audit_ids.flush(target_type, target_id)
        cls.get_audit_ids_by_date.flush(target_type, target_id, date)

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_audit_ids(cls, target_type, target_id):
        db = DBSession()
        rs = db.query(cls.audit_id).filter_by(
            target_id=target_id, target_type=target_type)
        return sorted((r[0] for r in rs), reverse=True)

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_audit_ids_by_date(cls, target_type, target_id, date):
        db = DBSession()
        rs = db.query(cls.audit_id).filter(
            cls.target_id == target_id,
            cls.target_type == target_type,
            cast(cls.created_at, DATE) == date
        )
        return sorted((r[0] for r in rs), reverse=True)


class AuditIndexInstance(TimestampMixin, UpsertMixin, DeclarativeBase):

    __tablename__ = 'audit_index_instance'
    __table_args__ = (
        Index('ux_audit_instance_key', 'application_id', 'instance_key',
              'instance_type', 'cluster_name', 'audit_id', unique=True),
        DeclarativeBase.__table_args__,
    )

    id = Column(BigInteger, primary_key=True)
    audit_id = Column(BigInteger, nullable=False)
    application_id = Column(Integer, nullable=False)
    cluster_name = Column(Unicode(64, collation='utf8mb4_bin'),
                          nullable=False)
    instance_key = Column(Unicode(128, collation='utf8mb4_bin'),
                          nullable=False)
    instance_type = Column(TINYINT, nullable=False)

    TYPE_CHOICES = (TYPE_CONFIG, TYPE_SWITCH, TYPE_SERVICE)

    @classmethod
    def create(cls, db, audit_id, created_at, instance_type, application_id,
               cluster_name, key):
        assert instance_type in cls.TYPE_CHOICES
        stmt = cls.upsert().values(
            audit_id=audit_id, application_id=application_id,
            cluster_name=cluster_name, instance_key=key,
            instance_type=instance_type)
        db.execute(stmt)

    @classmethod
    @cache_on_arguments(10 * 60)
    def get_audit_ids(cls, instance_type, application_id, cluster_name,
                      instance_key):
        """Get ids of :class:`.audit.AuditLog` by instance information.
        Regardless of cluster if the ``cluster_name`` is ``None``
        """
        db = DBSession()
        rs = db.query(cls.audit_id).filter_by(
            instance_type=instance_type, application_id=application_id,
            cluster_name=cluster_name, instance_key=instance_key)
        return sorted((r[0] for r in rs), reverse=True)

    @classmethod
    def flush_cache(cls, date, instance_type, application_id, cluster_name,
                    instance_key):
        cls.get_audit_ids.flush(
            instance_type, application_id, cluster_name, instance_key)


INDEX_MODELS_MAP = {
    TYPE_SITE: AuditIndex,
    TYPE_TEAM: AuditIndex,
    TYPE_APPLICATION: AuditIndex,
    TYPE_CONFIG: AuditIndexInstance,
    TYPE_SWITCH: AuditIndexInstance,
    TYPE_SERVICE: AuditIndexInstance,
}


def create_index(db, audit_id, created_at, index):
    model = INDEX_MODELS_MAP[index[0]]
    model.create(db, audit_id, created_at, *index)


def flush_index_cache(date, index):
    model = INDEX_MODELS_MAP[index[0]]
    model.flush_cache(date, *index)
