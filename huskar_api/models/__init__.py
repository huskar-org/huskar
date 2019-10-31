from __future__ import absolute_import

import datetime

from huskar_sdk_v2.bootstrap.client import BaseClient
from huskar_sdk_v2.consts import BASE_PATH
from sqlalchemy import text, Column, DateTime
from sqlalchemy.ext.declarative import declared_attr

from huskar_api import settings
from huskar_api.settings import ZK_SETTINGS
from huskar_api.models.cache import Cache, cache_mixin
from huskar_api.models.db import model_base, db_manager
from .utils import make_cache_decorator

__all__ = ['huskar_client', 'DBSession', 'DeclarativeBase', 'TimestampMixin',
           'cache_manager', 'cache_on_arguments', 'CacheMixin']

#: The client of Huskar SDK which manages the ZooKeeper sessions
huskar_client = BaseClient(
    ZK_SETTINGS['servers'], ZK_SETTINGS['username'], ZK_SETTINGS['password'],
    base_path=BASE_PATH, max_retries=-1)
huskar_client.start(ZK_SETTINGS['start_timeout'])

#: The scoped session factory of SQLAlchemy
DBSession = db_manager.get_session('default')

#: The base class of SQLAlchemy declarative model
DeclarativeBase = model_base()
DeclarativeBase.__table_args__ = {
    'mysql_character_set': 'utf8mb4', 'mysql_collate': 'utf8mb4_bin'}

cache_manager = Cache(settings.CACHE_SETTINGS['default'])

#: The Redis raw client
redis_client = cache_manager.make_client(raw=True)
#: The decorator of Redis cache
cache_on_arguments = make_cache_decorator(cache_manager.make_client(raw=True))
#: The mixin class of Redis cache
CacheMixin = cache_mixin(
    cache=cache_manager.make_client(namespace='%s:v2' % __name__),
    session=DBSession)
CacheMixin.TABLE_CACHE_EXPIRATION_TIME = settings.TABLE_CACHE_EXPIRATION_TIME


class TimestampMixin(object):
    """The mixin class of timestamp field.

    For the requirement of DBA, all new models which use MySQL should include
    this class as one of their bases.
    """

    @declared_attr
    def created_at(cls):
        return Column(
            DateTime, nullable=False, default=datetime.datetime.now,
            server_default=text('CURRENT_TIMESTAMP'), index=True)

    @declared_attr
    def updated_at(cls):
        return Column(
            DateTime, nullable=False, server_default=text(
                'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'),
            index=True)
