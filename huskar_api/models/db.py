from __future__ import absolute_import

import contextlib
import os
import sha
import functools
import random
import uuid
import time
import threading
import logging

import gevent

from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.types import Integer
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

from huskar_api import settings


logger = logging.getLogger(__name__)


class UpsertMixin(object):
    @classmethod
    def upsert(cls):
        """Build :class:`huskar_api.models.db.Upsert` statement.

        Example::

            class Foo(ModelBase, UpsertMixin):
                @classmethod
                def ensure(cls, name):
                    with DBSession() as db:
                        db.execute(cls.upsert().values(name=name))
        """
        return Upsert(cls.__table__)


class ModelMeta(DeclarativeMeta):
    def __new__(self, name, bases, attrs):
        cls = DeclarativeMeta.__new__(self, name, bases, attrs)

        from .cache import CacheMixinBase
        for base in bases:
            if issubclass(base, CacheMixinBase) and hasattr(cls, "_hook"):
                cls._hook.add(cls)
                break
        return cls


class Upsert(Insert):
    pass


@compiles(Upsert, 'mysql')
def mysql_upsert(insert_stmt, compiler, **kwargs):
    # A modified version of https://gist.github.com/timtadh/7811458.
    # The license (3-Clause BSD) is in the repository root.
    parameters = insert_stmt.parameters
    if insert_stmt._has_multi_parameters:
        parameters = parameters[0]  # pragma: no cover  # TODO: fix
    keys = list(parameters or {})
    pk = insert_stmt.table.primary_key
    auto = None
    if (len(pk.columns) == 1 and
            isinstance(pk.columns.values()[0].type, Integer) and
            pk.columns.values()[0].autoincrement):
        auto = pk.columns.keys()[0]
        if auto in keys:
            keys.remove(auto)
    insert = compiler.visit_insert(insert_stmt, **kwargs)
    ondup = 'ON DUPLICATE KEY UPDATE'
    updates = ', '.join(
        '%s = VALUES(%s)' % (c.name, c.name)
        for c in insert_stmt.table.columns
        if c.name in keys
    )
    if auto is not None:
        last_id = '%s = LAST_INSERT_ID(%s)' % (auto, auto)
        if updates:
            updates = ', '.join((last_id, updates))
        else:  # pragma: no cover  # TODO: fix
            updates = last_id
    upsert = ' '.join((insert, ondup, updates))
    return upsert


class RecycleField(object):
    def __get__(self, instance, klass):
        if instance is not None:
            return int(random.uniform(0.75, 1) * instance._origin_recyle)
        raise AttributeError   # pragma: no cover  # TODO: fix


def model_base(cls=object, **kwds):
    """Construct a base class for declarative class definitions, kwds params
    must be a subset of ``declarative_base`` params in sqlalchemy.

    :param cls:
      Atype to use as the base for the generated declarative base class.
      Defaults to :class:`object`. May be a class or tuple of classes.
    """
    return declarative_base(cls=cls, metaclass=ModelMeta, **kwds)


def comfirm_close_when_exception(exc):
    def wrapper(func):
        @functools.wraps(func)
        def comfirm_close(self, *args, **kwds):
            current_transactions = tuple()
            if self.transaction is not None:
                current_transactions = self.transaction._iterate_parents()
            try:
                func(self, *args, **kwds)
            except exc:
                logger.debug("Exception occurred and close connections.")
                close_connections(
                    self.engines.itervalues(), current_transactions)
                raise
        return comfirm_close
    return wrapper


db_ctx = threading.local()


def scope_func():
    if not getattr(db_ctx, 'session_stack', None):
        db_ctx.session_stack = 0
    return (threading.current_thread().ident, db_ctx.session_stack)


@contextlib.contextmanager
def session_stack():
    if not getattr(db_ctx, 'session_stack', None):
        db_ctx.session_stack = 0

    try:
        db_ctx.session_stack += 1
        yield
    finally:
        db_ctx.session_stack -= 1


class RoutingSession(Session):
    _name = None
    CLOSE_ON_EXIT = True

    def __init__(self, engines, *args, **kwds):
        super(RoutingSession, self).__init__(*args, **kwds)
        self.engines = engines
        self.slave_engines = [e for role, e in engines.items()
                              if role != 'master']
        assert self.slave_engines, ValueError("DB slave configs is wrong!")
        self._id = self.gen_id()
        self._close_on_exit = self.CLOSE_ON_EXIT

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_val is None:
                self.flush()
                self.commit()
            elif isinstance(exc_val, SQLAlchemyError):
                self.rollback()   # pragma: no cover  # TODO: fix
        except SQLAlchemyError:
            self.rollback()
            raise
        finally:
            if self._close_on_exit:
                self.close()
            self._close_on_exit = self.CLOSE_ON_EXIT

    def close_on_exit(self, value):
        self._close_on_exit = bool(value)
        return self

    def get_bind(self, mapper=None, clause=None):
        if self._name:
            return self.engines[self._name]
        elif self._flushing:
            return self.engines['master']
        else:
            return random.choice(self.slave_engines)

    def using_bind(self, name):
        self._name = name
        return self

    @comfirm_close_when_exception(gevent.Timeout)
    def commit(self):
        super(RoutingSession, self).commit()

    @comfirm_close_when_exception(gevent.Timeout)
    def flush(self):
        super(RoutingSession, self).flush()

    @comfirm_close_when_exception(BaseException)
    def rollback(self):
        with gevent.Timeout(5):
            super(RoutingSession, self).rollback()

    @comfirm_close_when_exception(BaseException)
    def close(self):
        with gevent.Timeout(5):
            super(RoutingSession, self).close()

    def gen_id(self):
        pid = os.getpid()
        tid = threading.current_thread().ident
        clock = time.time() * 1000
        address = id(self)
        hash_key = self.hash_key
        return sha.new('{0}\0{1}\0{2}\0{3}\0{4}'.format(
            pid, tid, clock, address, hash_key)).hexdigest()[:20]


def make_session(engines, force_scope=False, info=None):
    if settings.IS_IN_DEV or force_scope:
        scopefunc = scope_func
    else:
        scopefunc = None

    session = scoped_session(
        sessionmaker(
            class_=RoutingSession,
            expire_on_commit=False,
            engines=engines,
            info=info or {"name": uuid.uuid4().hex},
        ),
        scopefunc=scopefunc
    )
    return session


def close_connections(engines, transactions):
    if engines and transactions:
        for engine in engines:
            for parent in transactions:
                conn = parent._connections.get(engine)
                if conn:
                    conn[0].invalidate()


def patch_engine(engine):
    pool = engine.pool
    pool._origin_recyle = pool._recycle
    del pool._recycle
    setattr(pool.__class__, '_recycle', RecycleField())
    return engine


class DBManager(object):
    def __init__(self):
        self.session_map = {}
        self.create_sessions()

    def create_sessions(self):
        if not settings.DB_SETTINGS:
            raise ValueError('DB_SETTINGS is empty')
        for db, db_configs in settings.DB_SETTINGS.iteritems():
            self.add_session(db, db_configs)

    def get_session(self, name):
        try:
            return self.session_map[name]
        except KeyError:
            raise KeyError(
                '`%s` session not created, check `DB_SETTINGS`' % name)

    def add_session(self, name, config):
        if name in self.session_map:
            raise ValueError("Duplicate session name {},"
                             "please check your config".format(name))
        session = self._make_session(name, config)
        self.session_map[name] = session
        return session

    @classmethod
    def _make_session(cls, db, config):
        urls = config['urls']
        for name, url in urls.iteritems():
            assert url, "Url configured not properly for %s:%s" % (db, name)
        pool_size = config.get('pool_size', 10)
        max_overflow = config.get('max_overflow', 1)
        pool_recycle = config.get('pool_recycle', 300)
        engines = {
            role: cls.create_engine(dsn,
                                    pool_size=pool_size,
                                    max_overflow=max_overflow,
                                    pool_recycle=pool_recycle,
                                    execution_options={'role': role})
            for role, dsn in urls.iteritems()
        }
        return make_session(engines, info={"name": db})

    def close_sessions(self, should_close_connection=False):
        dbsessions = self.session_map
        for dbsession in dbsessions.itervalues():
            if should_close_connection:
                session = dbsession()
                if session.transaction is not None:
                    close_connections(session.engines.itervalues(),
                                      session.transaction._iterate_parents())
            try:
                dbsession.remove()
            except:  # pragma: no cover  # noqa
                logger.exception("Error closing session")

    @classmethod
    def create_engine(cls, *args, **kwds):
        engine = patch_engine(sqlalchemy_create_engine(*args, **kwds))
        return engine


db_manager = DBManager()
