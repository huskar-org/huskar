from __future__ import absolute_import

import bz2
import cPickle as pickle
import urlparse
import logging

from .client import FaultTolerantStrictRedis

logger = logging.getLogger(__name__)


# enable zlib with level 6
def zdumps(x):
    return bz2.compress(pickle.dumps(x, pickle.HIGHEST_PROTOCOL), 6)


def zloads(x):
    return pickle.loads(bz2.decompress(x))


class _RedisWrapper(object):
    def __init__(self, dsn, namespace=None, socket_timeout=1,
                 socket_connect_timeout=3, **kwargs):
        self.namespace = namespace
        self.client = FaultTolerantStrictRedis.from_url(
            dsn, socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout, **kwargs)

    def _keygen(self, raw_key):
        if self.namespace is None:
            return raw_key
        return "{0}:{1}".format(self.namespace, raw_key)

    def set(self, raw_key, val, expiration_time=None, dumps=zdumps):
        return self.client.set(self._keygen(raw_key), dumps(val),
                               expiration_time)

    def setnx(self, key, val, ex=None, nx=True, dumps=zdumps):
        return self.client.set(self._keygen(key), dumps(val), ex=ex, nx=nx)

    def msetnx(self, mapping, ex=None, nx=True, dumps=zdumps):
        mapping = {self._keygen(k): dumps(v) for k, v in mapping.items()}
        with self.client.pipeline(transaction=False) as p:
            for k, v in mapping.items():
                p.set(k, v, ex=ex, nx=nx)
            p.execute()

    def mset(self, mapping, expiration_time=None, dumps=zdumps):
        if not mapping:
            return

        mapping = {self._keygen(k): dumps(v) for k, v in mapping.items()}
        if not expiration_time:
            self.client.mset(mapping)
        else:
            with self.client.pipeline(transaction=False) as pipe:
                for key, value in mapping.items():
                    pipe.set(key, value, expiration_time)
                pipe.execute()

    def get(self, raw_key, loads=zloads):
        val = self.client.get(self._keygen(raw_key))
        if val:
            return loads(val)

    def mget(self, raw_keys, loads=zloads):
        if not raw_keys:
            return []

        keys = (self._keygen(k) for k in raw_keys)
        return [loads(v) if v is not None else v
                for v in self.client.mget(keys)]

    def delete(self, *raw_keys):
        if not raw_keys:
            return

        keys = (self._keygen(k) for k in raw_keys)
        return self.client.delete(*keys)


class Cache(object):
    """Make dogpile region or wrapped redis client.

    Dogpile region is used for api cache.

    The wrapped redis client only support get and set. The values are pickled
    and compressed when stored to redis. This client is used for table cache.
    For general purpose redis client that stores values to backend redis
    cluster, the *raw* client should be used::

        cache = Cache("service_name", "test")

        # region
        region = cache.make_region()

        # forever region
        region = cache.make_region(expiration_time=None, lock_timeout=3,
                                   redis_lock=False, key_func=False)

        # table cache client
        client = cache.make_client()

        # raw client
        client = cache.make_client(raw=True)

    Settings::

        CACHE_SETTINGS          redis dsn dict

    :param name: service name, should set on settings
    :param namespace: cache client namespace
    """
    def __init__(self, url, namespace=None, socket_timeout=1,
                 socket_connect_timeout=3, max_pool_size=100, timeout=5):
        self._socket_timeout = socket_timeout
        self._socket_connect_timeout = socket_connect_timeout
        self._max_connections = max_pool_size
        self._timeout = timeout
        self._dsn = url
        parsed = urlparse.urlparse(self._dsn)
        self.host = parsed.hostname
        self.port = parsed.port
        self.db = int(parsed.path[1:]) if parsed.path else 0
        self.namespace = namespace

    def make_client(self, raw=False, namespace=None, socket_timeout=None,
                    socket_connect_timeout=None):
        """Make a redis client.

        :param raw: whether make a wrapped client or not
        :param namespace: customized namespace
        :param socket_timeout: socket timeout
        :param socket_connect_timeout: socket connect timeout

            >>> client = self.make_client('test', socket_timeout=5,
                                          socket_connect_timeout=5)
            >>> client.get('hello')

        If raw set to ``True``, a redis client is returned. This client can be
        used as a normal redis client.
        If ``False``, a wrapped redis client is returned. This client only
        support get and set and used for table cache.
        """
        socket_timeout = socket_timeout or self._socket_timeout
        socket_connect_timeout = socket_connect_timeout or \
            self._socket_connect_timeout

        if raw and namespace:
            raise ValueError("`raw` and `namespace` can't both be set")

        if raw:
            return FaultTolerantStrictRedis.from_url(
                self._dsn, socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                max_connections=self._max_connections,
                timeout=self._timeout)

        return _RedisWrapper(self._dsn, namespace or self.namespace,
                             socket_timeout, socket_connect_timeout,
                             max_connections=self._max_connections,
                             timeout=self._timeout)
