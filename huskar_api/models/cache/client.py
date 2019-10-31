from __future__ import absolute_import

import types
import logging
import functools
import itertools

from redis import RedisError
from redis.client import StrictPipeline, StrictRedis
from redis.connection import ConnectionPool


logger = logging.getLogger(__name__)


class RedisConnectionPoolMixin(object):
    """
    A mixin to change redis connection pool to :class:`.BlockingConnectionPool`

    :param url: redis dsn
    :param db: redis db
    :param kwargs: redis connection kwargs
    :return: redis client

    it's same as official usage:

        client = redis.StrictRedis(
            connection_pool=BlockingonnectionPool.from_url(urCl))

    """

    @classmethod
    def from_url(cls, url, db=None, **kwargs):

        if 'max_connections' not in kwargs:
            kwargs['max_connections'] = 100

        kwargs.pop('timeout', None)

        connection_pool = ConnectionPool.from_url(url, db=db, **kwargs)
        return cls(connection_pool=connection_pool)


class _FaultTolerantMeta(type):
    @staticmethod
    def _deco(func, fallback):
        @functools.wraps(func)
        def wrapper(self, *args, **kw):
            orig_args = args

            if func.__name__ == "mget":
                # convert first generator arg to list
                if args and isinstance(args[0], types.GeneratorType):
                    args = list(args)
                    args[0] = list(args[0])
                    orig_args = args

                # in case of in-place change of args when `func` called
                if len(args) > 1 and isinstance(args[0], list):
                    orig_args = list(args)
                    orig_args[0] = args[0][:]

            try:
                return func(self, *orig_args, **kw)
            except RedisError as e:
                a = ", ".join(repr(a) for a in args) if args else ''
                k = ", ".join("{}={!r}".format(k, v)
                              for k, v in kw.items()) if kw else ''
                s = ", " if a and k else ''

                logger.warning("Err: {}. Failed to call cache api: {}({})"
                               .format(repr(e), func.__name__, a + s + k))

                return fallback(*args, **kw)
        return wrapper

    def __new__(self, name, bases, attrs):
        cls = type.__new__(self, name, bases, attrs)

        methods = cls.__covered_methods__
        for method, fallback in methods:
            origin = getattr(cls, method)
            setattr(cls, method, self._deco(origin, fallback))
        return cls


def _mget_fb(keys, *args):
    if isinstance(keys, (basestring, bytes)):
        keys = [keys]
    return list(itertools.repeat(None, len(list(keys)) + len(args)))


def _none(*args, **kwargs):
    return None


def _false(*args, **kwargs):
    return False


def _zero(*args, **kwargs):
    return 0


class FaultTolerantStrictRedis(RedisConnectionPoolMixin, StrictRedis):
    """Return fallbacks when actual methods failed.

    .. note::

        ``help(<method>)`` can not get true function signature of original
        methods.
    """
    __metaclass__ = _FaultTolerantMeta

    __covered_methods__ = [
        ("get", _none),
        ("set", _false),
        ("setex", _false),
        ("setnx", _false),
        ("mget", _mget_fb),
        ("mset", _false),
        ("delete", _false),
        ("incr", _zero),
        ("incrby", _zero),
        ("incrbyfloat", _zero),
        ("decr", _zero),
        ("expire", _false),
        ("ttl", _zero),
    ]

    def __init__(self, **kwargs):
        if kwargs.get('connection_pool') is not None:
            self.host = kwargs['connection_pool'].connection_kwargs.\
                get('host', 'UNKOWN')
            self.port = kwargs['connection_pool'].connection_kwargs.\
                get('port', 0)
        else:
            self.host = kwargs.get('host', 'localhost')
            self.port = kwargs.get('port', 6379)
        self.url = "%s_%s" % (self.host.replace('.', '_'), self.port)
        kwargs['max_connections'] = 100
        super(FaultTolerantStrictRedis, self).__init__(**kwargs)

    def pipeline(self, transaction=False, shard_hint=None):
        return FaultTolerantPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)

    def execute_command(self, *args, **options):
        res = super(FaultTolerantStrictRedis,
                    self).execute_command(*args, **options)
        return res


class FaultTolerantPipeline(StrictPipeline):

    def execute(self, raise_on_error=True):
        commands = len(self.command_stack)

        try:
            return super(FaultTolerantPipeline,
                         self).execute(raise_on_error)
        except Exception as exc:
            logger.exception(exc)
            return list(itertools.repeat(None, commands))
