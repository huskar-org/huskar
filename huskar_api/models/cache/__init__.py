from __future__ import absolute_import

import itertools
import logging

import redis
import sqlalchemy.exc as sa_exc

from sqlalchemy.orm import attributes
from sqlalchemy.orm.util import identity_key

from .region import Cache
from .hook import EventHook


__all__ = ["Cache", "cache_mixin", "CacheMixinBase"]


logger = logging.getLogger(__name__)


def _dict2list(ids, os):
    return [os[i] for i in ids if i in os]


def make_transient_to_detached(instance):
    '''
    Moved from sqlalchemy newer version
    '''
    state = attributes.instance_state(instance)
    if state.session_id or state.key:
        raise sa_exc.InvalidRequestError(
            "Given object must be transient")
    state.key = state.mapper._identity_key_from_state(state)
    if state.deleted:  # pragma: no cover
        del state.deleted
    state._commit_all(state.dict)
    state._expire_attributes(state.dict, state.unloaded)


class _Failed(object):
    def __get__(self, obj, type=None):
        raise NotImplementedError


class CacheMixinBase(object):
    """Cache base class.

    Never use this class explicitly, use :func:`cache_mixin` instead.

    Settings::

        TABLE_CACHE_EXPIRATION_TIME    cached object expiration time

        RAWDATA_VERSION                raw data cache version

    .. important::

        When model schema or thrift struct changed,
        ``RAWDATA_VERSION`` should be updated to invalidate all caches.

    """
    # pylint: disable=no-member

    TABLE_CACHE_EXPIRATION_TIME = None

    RAWDATA_VERSION = None

    _cache_client = _Failed()
    _db_session = _Failed()
    _set_fail_callbacks = set()

    def __repr__(self):
        return "<%s|%s %s>" % (self.__tablename__, self.pk, hex(id(self)))

    @property
    def __rawdata__(self):
        return {c.name: getattr(self, c.name)
                for c in self.__table__.columns}

    @classmethod
    def gen_raw_key(cls, pk):
        """Key without namespace prefix.

        :param pk:  table's primary key
        :return: generated key
        """
        if cls.RAWDATA_VERSION:
            return "{0}|{1}|{2}".format(
                cls.__tablename__, pk, cls.RAWDATA_VERSION)
        return "{0}|{1}".format(cls.__tablename__, pk)

    @classmethod
    def _from_cache(cls, pks, from_raw=False):
        _hit_counts = 0
        _objs = {}
        try:
            vals = cls._cache_client.mget(
                cls.gen_raw_key(id)
                for id in pks)
            if vals:
                if from_raw:
                    cached = {
                        k: cls.from_cache(v)
                        for k, v in zip(pks, vals)
                        if v is not None
                    }
                else:
                    cached = {
                        k: v
                        for k, v in zip(pks, vals)
                        if v is not None
                    }
                _objs.update(cached)
                _hit_counts = len(cached)
                cls._statsd_incr("hit", _hit_counts)
        except redis.ConnectionError as e:
            logger.error(e)
        except TypeError as e:
            logger.error(e)

        return _objs

    @property
    def pk(self):
        """Get object primary key.

        :return: primary key value
        """
        return getattr(self, self.__mapper__.primary_key[0].name)

    @classmethod
    def pk_name(cls):
        """Get object primary key name. e.g. `id`"""
        if cls.__mapper__.primary_key:
            return cls.__mapper__.primary_key[0].name

    @classmethod
    def pk_attribute(cls):
        """Get object primary key attribute.

        :return: sqlalchemy ``Column`` object of the primary key
        """
        if cls.__mapper__.primary_key:
            return getattr(cls, cls.__mapper__.primary_key[0].name)

    @classmethod
    def register_set_fail_callback(cls, callback, raise_exc=False):
        """Register callback to invoke when set cache failed.

        Usage:

          def callback(data_obj, model, primary_key, val):
              do_something()

          UsageModel.register_set_fail_callback(callback)

        """
        cls._set_fail_callbacks.add((callback, raise_exc))

    @classmethod
    def clear_set_fail_callbacks(cls):
        cls._set_fail_callbacks.clear()

    @classmethod
    def _call_set_fail_callbacks(cls, data, key, val):
        for func, raise_exc in cls._set_fail_callbacks:
            try:
                func(data, cls, key, val)
            except Exception as e:
                if raise_exc:
                    raise
                logger.error(e)

    @classmethod
    def _statsd_incr(cls, key, val=1):
        # TODO
        pass

    @classmethod
    def flush(cls, ids):
        """Delete cache values.

        :param ids: a list of primary key values
        """
        keys = itertools.chain(*[
            (
                cls.gen_raw_key(i),
            ) for i in ids])
        return cls._cache_client.delete(*keys)

    @classmethod
    def from_cache(cls, rawdata):
        """Create object from rawdata

        :param rawdata: dict value of the object
        :return: object
        """
        obj = cls(**rawdata)
        obj._cached = True
        make_transient_to_detached(obj)
        cls._db_session.add(obj)
        return obj

    @classmethod
    def get(cls, _id, force=False):
        """Query object by pk.

        :func:`~get` will use db slave and store the value to cache.

        Use ``force=True`` to force load from db.

        :param _id: primary key value
        :param force: whether force to load from db
        :return: object from db or cache
        """
        if not force:
            # try load from session
            ident_key = identity_key(cls, _id)
            if cls._db_session.identity_map and \
                    ident_key in cls._db_session.identity_map:
                return cls._db_session.identity_map[ident_key]

            try:
                cached_val = cls._cache_client.get(cls.gen_raw_key(_id))
                if cached_val:
                    cls._statsd_incr("hit")

                    # load from cache
                    return cls.from_cache(cached_val)
            except redis.ConnectionError as e:
                logger.error(e)
            except TypeError as e:
                logger.error(e)

        cls._statsd_incr("miss")

        obj = cls._db_session().query(cls).get(_id)
        if obj is not None:
            cls.set_raw(obj.__rawdata__, nx=True)
        return obj

    @classmethod
    def mget(cls, _ids, force=False, as_dict=False):
        """Query a list of objects by pks.

        :func:`~mget` will always use db slave, values will be stored
        to cache if cache misses.

        Use ``force=True`` to force load from db.

        :param _ids: a list of pks
        :param force: whether force to load from db
        :param as_dict: return dict or list
        :return: dict or list of objects
        """
        if not _ids:
            return {} if as_dict else []

        objs = {}
        if not force:
            # load from session
            if cls._db_session.identity_map:
                for i in _ids:
                    ident_key = identity_key(cls, i)
                    if ident_key in cls._db_session.identity_map:
                        objs[i] = cls._db_session.identity_map[ident_key]

            # load from cache
            if len(_ids) > len(objs):
                missed_ids = list(set(_ids) - set(objs))
                _objs = cls._from_cache(missed_ids, from_raw=True)
                objs.update(_objs)

        lack_ids = set(_ids) - set(objs)
        if lack_ids:
            pk = cls.pk_attribute()
            # we assume CacheMixin have pk, if not, bypass it.
            if pk:
                lack_objs = cls._db_session().using_bind('master').\
                    query(cls).filter(pk.in_(lack_ids)).all()
                if lack_objs:
                    cls.mset(lack_objs, nx=True)

                cls._statsd_incr("miss", len(lack_ids))

                objs.update({obj.pk: obj for obj in lack_objs})
            else:  # pragma: no cover
                logger.warn("No pk found for %s, skip %s" %
                            cls.__tablename__, lack_ids)

        # TODO hack to make mget return list
        return objs if as_dict else _dict2list(_ids, objs)

    @classmethod
    def mget_cache_only(cls, _ids, as_dict=False):
        if not _ids:
            return {} if as_dict else []

        _objs = cls._from_cache(_ids, from_raw=True)
        return _objs if as_dict else _dict2list(_ids, _objs)

    @classmethod
    def _set(cls, val, expiration_time=None, nx=False):
        if not val:
            return

        pk_name = cls.pk_name()
        ttl = expiration_time or cls.TABLE_CACHE_EXPIRATION_TIME
        key = cls.gen_raw_key(val[pk_name])
        if nx:
            return cls._cache_client.setnx(key, val, ex=ttl, nx=nx)
        return cls._cache_client.set(key, val, expiration_time=ttl)

    @classmethod
    def set_raw(cls, raw_val, expiration_time=None, nx=False):
        """Store raw value to cache.

        :param raw_val: dict value of object
        :param expiration_time: redis expiration time
        """
        return cls._set(raw_val, expiration_time=expiration_time, nx=nx)

    @classmethod
    def set(cls, val, expiration_time=None, nx=False):
        """Store both raw value to cache.

        :param val: object
        :param expiration_time: redis expiration time
        """
        assert isinstance(val, cls)
        cls.set_raw(val.__rawdata__, expiration_time, nx=nx)

    @classmethod
    def _mset(cls, vals, nx=False):
        if not vals:
            return
        objs = {
            cls.gen_raw_key(val.pk): val.__rawdata__ for val in vals
        }
        ttl = cls.TABLE_CACHE_EXPIRATION_TIME
        if nx:
            cls._cache_client.msetnx(objs, ex=ttl, nx=nx)
            return
        cls._cache_client.mset(objs, expiration_time=ttl)
        return

    @classmethod
    def mset(cls, vals, nx=False):
        """Store a list of objects to cache.

        :param vals: a list of objects
        """
        if not vals:
            return

        assert isinstance(vals[0], cls)
        cls._mset_raw(vals, nx=nx)

    @classmethod
    def _mset_raw(cls, vals, nx=False):
        cls._mset(vals, nx=nx)


def cache_mixin(cache, session):
    """CacheMixin factory

    :param cache: cache region to store cached objects
    :param session: sqlalchemy session related to the model

    :return: subclass of :class:`CacheMixinBase`
    """

    hook = EventHook([cache], session)

    class _Cache(CacheMixinBase):
        _hook = hook

        _cache_client = cache
        _db_session = session
    return _Cache
