from __future__ import absolute_import

from sqlalchemy import Column, Integer, Unicode, UniqueConstraint

from huskar_api.models.db import UpsertMixin

from . import (
    DeclarativeBase, TimestampMixin, CacheMixin, DBSession, cache_on_arguments)


__all__ = ['Comment', 'set_comment', 'get_comment']


class Comment(TimestampMixin, CacheMixin, UpsertMixin, DeclarativeBase):
    __tablename__ = 'key_comment'
    __table_args__ = (
        UniqueConstraint(
            'key_type', 'application', 'cluster', 'key_name',
            name='uq_comment'
        ),
        DeclarativeBase.__table_args__,
    )

    TYPE_CHOICES = frozenset(['switch', 'config'])

    id = Column(Integer, primary_key=True)
    application = Column(Unicode(128, collation='utf8mb4_bin'),
                         nullable=False)
    cluster = Column(Unicode(64, collation='utf8mb4_bin'), nullable=False)
    key_type = Column(Unicode(16, collation='utf8mb4_bin'), nullable=False)
    key_name = Column(Unicode(128, collation='utf8mb4_bin'), nullable=False)
    key_comment = Column(Unicode(2048, collation='utf8mb4_bin'),
                         nullable=False)

    @classmethod
    def create(cls, application, cluster, key_type, key_name, key_comment):
        assert key_type in cls.TYPE_CHOICES
        stmt = cls.upsert().values(
            application=application,
            cluster=cluster,
            key_type=key_type,
            key_name=key_name,
            key_comment=key_comment,
        )
        with DBSession() as db:
            rs = db.execute(stmt)

        instance = cls.get(rs.lastrowid)
        DBSession().refresh(instance)

        cls.flush([instance.id])
        cls.find_id.flush(application, cluster, key_type, key_name)

        return instance

    @classmethod
    def find(cls, application, cluster, key_type, key_name):
        assert key_type in cls.TYPE_CHOICES
        comment_id = cls.find_id(application, cluster, key_type, key_name)
        if comment_id:
            return cls.get(comment_id)

    @classmethod
    @cache_on_arguments(5 * 60)
    def find_id(cls, application, cluster, key_type, key_name):
        assert key_type in cls.TYPE_CHOICES
        return DBSession().query(cls.id).filter_by(
            application=application,
            cluster=cluster,
            key_type=key_type,
            key_name=key_name,
        ).scalar()

    @classmethod
    def delete(cls, application, cluster, key_type, key_name):
        comment = cls.find(application, cluster, key_type, key_name)
        if comment is None:
            return
        with DBSession() as db:
            db.delete(comment)
        cls.flush([comment.id])
        cls.find_id.flush(application, cluster, key_type, key_name)


def get_comment(application, cluster, key_type, key_name, default=u''):
    """Gets the comment of specific key.

    :param str application: The name of application.
    :param str cluster: The name of cluster.
    :param str key_type: ``"config"`` or ``"switch"``.
    :pramm str default: The value will be returned if comment does not exist.
    :returns: The comment content.
    """
    comment = Comment.find(application, cluster, key_type, key_name)
    if comment:
        return comment.key_comment
    return default


def set_comment(application, cluster, key_type, key_name, value):
    """Sets the comment of specific key.

    :param str application: The name of application.
    :param str cluster: The name of cluster.
    :param str key_type: ``"config"`` or ``"switch"``.
    :param str value: The content of comment. If falsey value be passed, the
                      matched comment will be removed.
    """
    value = unicode(value).strip() if value else None
    if value:
        return Comment.create(application, cluster, key_type, key_name, value)
    Comment.delete(application, cluster, key_type, key_name)
