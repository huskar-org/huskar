from __future__ import absolute_import

# import cPickle as pickle
# import logging
import types
import socket

# from meepo2.signals import signal
import pytest
from redis import RedisError, ConnectionError
from redis import StrictRedis
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.exc as sa_exc

from huskar_api import settings
from huskar_api.models import DBSession, CacheMixin
from huskar_api.models.auth import Team
from huskar_api.models.cache import CacheMixinBase, make_transient_to_detached
from huskar_api.models.cache.hook import EventHook
from huskar_api.models.cache.region import _RedisWrapper, Cache
from huskar_api.models.cache.client import FaultTolerantStrictRedis


def get_unused_port():
    s = socket.socket()
    s.bind(('', 0))
    _, port = s.getsockname()
    s.close()
    return port


def test_fault_tolerant_redis_client_return_null_value(mocker):
    def raise_error(self, *args, **kwargs):
        if len(args) > 0 and isinstance(args[0], types.GeneratorType):
            list(args[0])
        raise RedisError

    client = FaultTolerantStrictRedis(port=0)
    for meth, _ in FaultTolerantStrictRedis.__covered_methods__:
        mocker.patch.object(StrictRedis, meth, raise_error)

    assert client.get('k') is None
    assert client.setex('k', '120', 120) is False
    assert client.setnx('k', '120') is False
    assert client.set('k', 'v') is False
    assert client.mget(['k', 'v', 'm']) == [None, None, None]
    assert client.mset(a='k') is False
    assert client.delete('k') is False

    assert client.incr('k') == 0
    assert client.incrby('k', 2) == 0
    assert client.incrbyfloat('k', 2.0) == 0
    assert client.decr('k') == 0

    def gen():
        yield 'k'
        yield 'v'
        yield 'm'

    assert client.mget(gen()) == [None, None, None]
    assert client.mget(gen(), 'u') == [None, None, None, None]
    assert client.mget(['k', 'v', 'm'], 'u') == [None, None, None, None]
    assert client.mget('k') == [None]


def test_fault_tolerant_redis_client_work_return_right_value():
    client = FaultTolerantStrictRedis.from_url(
        settings.CACHE_SETTINGS['default'])
    host = client.connection_pool.get_connection('info').host
    port = client.connection_pool.get_connection('info').port
    client = FaultTolerantStrictRedis(host=host, port=port)
    client.set('k', '1')
    assert client.get('k') == '1'


def test_fault_tolerant_pipeline():
    r = FaultTolerantStrictRedis(port=get_unused_port(), socket_timeout=1)
    with r.pipeline(transaction=False) as pipe:
        for i in range(3):
            pipe.set(str(i), '\x23\x98{}'.format(i), 10)
        res = pipe.execute()

    assert res == [None, None, None]


def test_strict_pipeline(mocker):
    r = FaultTolerantStrictRedis(port=get_unused_port(), socket_timeout=1)

    mock_zadd = mocker.MagicMock()
    mocker.patch.object(StrictRedis, 'zadd', mock_zadd)
    r.zadd('test', 2.234, 'member')
    mock_zadd.assert_called_with('test', 2.234, 'member')

    mock_pipe_zadd = mocker.MagicMock()
    mocker.patch.object(StrictRedis, 'zadd', mock_pipe_zadd)
    p = r.pipeline(transaction=False)
    p.zadd('test', 1.234, 'member2')
    mock_pipe_zadd.assert_called_with('test', 1.234, 'member2')


Base = declarative_base()


class MockSession(object):
    identity_map = []

    def __init__(self, return_value):
        self.return_value = return_value

    def all(self):
        return self.return_value

    def get(self, *args):
        return self.return_value

    def __getattr__(self, attr):
        return self

    def __call__(self, *args):
        return self


def test_set_raw(mocker):
    t = Team(id=0, team_name="hello")

    mock_set = mocker.MagicMock()
    mocker.patch.object(_RedisWrapper, "set", mock_set)
    Team.set_raw(t.__rawdata__, expiration_time=900)

    mock_set.assert_called_with(
        "team|0", {'id': 0, 'status': None,
                   'team_desc': None, 'team_name': 'hello'},
        expiration_time=900)


def test_mixin_set(mocker):
    t = Team(id=0, team_name="hello")

    set_raw_mock = mocker.MagicMock()
    mocker.patch.object(Team, "set_raw", set_raw_mock)

    Team.set(t, expiration_time=900)
    assert set_raw_mock.called


def test_mset(mocker):
    t1 = Team(id=0, team_name="hello")
    t2 = Team(id=1, team_name="world")

    mset = mocker.MagicMock()
    mocker.patch.object(_RedisWrapper, "mset", mset)
    Team.mset([t1, t2])

    expected = mocker.call(
        {"team|0": t1.__rawdata__, "team|1": t2.__rawdata__},
        expiration_time=Team.TABLE_CACHE_EXPIRATION_TIME)
    mset.assert_has_calls([expected])


def test_mset_with_wrapper(mocker):
    r = _RedisWrapper(settings.CACHE_SETTINGS['default'])
    data = {"hello": 90, "world": 123}
    r.mset(data, expiration_time=10)
    assert r.mget(["hello", "world"]) == [90, 123]

    r.mset(data)
    assert r.mget(["hello", "world"]) == [90, 123]


def test_get_from_session(mocker):
    session = DBSession

    mocker.patch.object(CacheMixin, "_db_session", DBSession)

    team = Team(id=0, team_name="test_get_from_session")
    session.add(team)
    session.commit()
    t = Team.get(0)
    assert t is team
    session.close()


def test_get_from_cache(mocker):
    mocker.patch.object(CacheMixin, "_db_session", DBSession)

    t = Team(id=0, team_name="test_get_from_cache")
    with mocker.patch.object(_RedisWrapper, "get", return_value=t.__rawdata__):
        m = Team.get(0)
        assert m._cached


def test_from_cache(mocker):
    t = Team(id=0, team_name='test')
    _key = 0

    with mocker.patch.object(_RedisWrapper, "mget",
                             return_value=t.__rawdata__):
        m = Team._from_cache([_key])
        assert _key in m


def test_get_from_db(mocker):
    t = Team(id=0, team_name="hello")

    mocker.patch.object(_RedisWrapper, "get", mocker.Mock(return_value=None))
    mocker.patch.object(CacheMixin, "_db_session", MockSession(t))

    r = Team.get(0)
    assert r is t


def test_new_cache_get(mocker):
    t = Team(id=0, team_name="hello")

    mocker.patch.object(_RedisWrapper, "get", mocker.Mock(return_value=None))
    mocker.patch.object(CacheMixin, "_db_session", MockSession(t))

    mock_set_raw = mocker.MagicMock()
    mocker.patch.object(Team, "set_raw", mock_set_raw)
    v = Team.get(0)
    mock_set_raw.assert_called_with(v.__rawdata__, nx=True)
    assert v is t


def test_mget_from_session(mocker):
    session = DBSession
    mocker.patch.object(CacheMixin, "_db_session", DBSession)

    t1 = Team(id=0, team_name="hello")
    t2 = Team(id=1, team_name="world")

    for t in (t1, t2):
        session.add(t)
        session.commit()

    r = Team.mget([0, 1])
    assert r[0] is t1 and r[1] is t2
    session.close()


def test_mget_from_cache(mocker):
    mocker.patch.object(CacheMixin, "_db_session", DBSession)

    t1 = Team(id=0, team_name="hello")
    t2 = Team(id=1, team_name="world")

    with mocker.patch.object(_RedisWrapper, "mget",
                             return_value=[t1.__rawdata__, t2.__rawdata__]):
        m = Team.mget([0, 1])
        assert all([t._cached for t in m])


def test_mget_from_db(mocker):
    t1 = Team(id=0, team_name="hello")
    t2 = Team(id=1, team_name="world")

    mocker.patch.object(_RedisWrapper, "mget", mocker.Mock(return_value=None))
    mocker.patch.object(Team, "_db_session", MockSession([t1, t2]))

    r = Team.mget([0, 1, 2])
    assert len(r) == 2
    assert r[0] is t1 and r[1] is t2


def test_new_cache_mget(mocker):
    t1 = Team(id=0, team_name="hello")
    t2 = Team(id=1, team_name="world")

    mocker.patch.object(_RedisWrapper, "mget", mocker.Mock(return_value=None))
    mocker.patch.object(Team, "_db_session", MockSession([t1, t2]))

    mset_raw = mocker.MagicMock()
    mocker.patch.object(Team, "_mset_raw", mset_raw)
    Team.mget([0, 1])
    mset_raw.assert_called_with([t1, t2], nx=True)


def test_mget_get_primary_key(mocker):
    t = Team(id=0, team_name="hello")

    mocker.patch.object(_RedisWrapper, "mget", mocker.Mock(return_value=None))
    mocker.patch.object(Team, "_db_session", MockSession([t]))

    pk = mocker.MagicMock()
    mocker.patch.object(Team, "pk", pk)
    a = Team.mget([0], as_dict=True)
    assert a == {pk: t}


def test_mget_cache_miss(mocker):
    t = Team(id=0, team_name="hello")

    mocker.patch.object(_RedisWrapper, "mget", mocker.Mock(return_value=None))
    mocker.patch.object(Team, "_db_session", MockSession([t]))

    incr = mocker.MagicMock()
    mocker.patch.object(Team, "_statsd_incr", incr)
    Team.mget([0])
    incr.assert_called_with("miss", 1)


def test_get_cache_miss(mocker):
    t = Team(id=0, team_name="hello")

    mocker.patch.object(_RedisWrapper, "get", mocker.Mock(return_value=None))
    mocker.patch.object(Team, "_db_session", MockSession(t))

    incr = mocker.MagicMock()
    mocker.patch.object(Team, "_statsd_incr", incr)
    Team.get(0)
    incr.assert_called_with("miss")


def test_mget_cache_hit(mocker):
    mocker.patch.object(CacheMixin, "_db_session", DBSession)

    t1 = Team(id=0, team_name="hello")
    t2 = Team(id=1, team_name="world")
    raw = [t.__rawdata__ for t in (t1, t2)]

    mocker.patch.object(_RedisWrapper, "mget", mocker.Mock(return_value=raw))

    # get
    incr = mocker.MagicMock()
    mocker.patch.object(Team, "_statsd_incr", incr)
    Team.mget([0, 1])
    incr.assert_called_with("hit", 2)


def test_get_cache_hit(mocker):
    mocker.patch.object(CacheMixin, "_db_session", DBSession)

    t = Team(id=0, team_name="hello")
    raw = t.__rawdata__

    mocker.patch.object(_RedisWrapper, "get", mocker.Mock(return_value=raw))

    # get
    incr = mocker.MagicMock()
    mocker.patch.object(Team, "_statsd_incr", incr)
    Team.get(0)
    incr.assert_called_with("hit")


def test_flush(mocker):
    delete = mocker.MagicMock()
    mocker.patch.object(_RedisWrapper, "delete", delete)
    Team.flush([0, 1])

    delete.assert_called_with("team|0", "team|1")


def test__keygen():
    raw_key = 'test_233_key'
    result = _RedisWrapper(settings.CACHE_SETTINGS['default'])._keygen(raw_key)
    assert result == raw_key


def test_set():
    client = _RedisWrapper(settings.CACHE_SETTINGS['default'])
    client.set('test_set', '233')
    assert client.get('test_set') == '233'
    client.delete('test_set')


def test_mset_ignore__RedisWrapper():
    client = _RedisWrapper(settings.CACHE_SETTINGS['default'])
    assert client.mset({}) is None


def test_mset_ignore_CacheMixinBase():
    assert CacheMixinBase.mset({}) is None


def test_mget_ignore__RedisWrapper():
    client = _RedisWrapper(settings.CACHE_SETTINGS['default'])
    assert client.mget([]) == []


def test_mget_ignore_CacheMixinBase():
    assert CacheMixinBase.mget([]) == []
    assert CacheMixinBase.mget([], as_dict=True) == {}


def test_delte_ignore():
    client = _RedisWrapper(settings.CACHE_SETTINGS['default'])
    assert client.delete() is None


def test_get_ignore():
    client = _RedisWrapper(settings.CACHE_SETTINGS['default'])
    assert client.get('test_get_ignore_233') is None


def test_make_client_error():
    client = Cache(settings.CACHE_SETTINGS['default'])
    with pytest.raises(ValueError) as error:
        client.make_client('a', 'b')

    assert str(error.value) == "`raw` and `namespace` can't both be set"


@pytest.fixture
def hook(redis_client):
    return EventHook([redis_client], DBSession)


def test_update_cache_fail_hook(hook, mocker):
    callback_confirm = []

    def callback(data_obj, model, pk, val):
        callback_confirm.append((data_obj, model, pk, val))

    # register callback
    Team.register_set_fail_callback(callback)
    m = Team(id=0, team_name="OK")

    mocker.patch.object(CacheMixinBase, '_cache_client', mocker.MagicMock())
    # perform an exception
    mocker.patch.object(CacheMixinBase, 'set_raw',
                        mocker.MagicMock(side_effect=Exception))

    with pytest.raises(Exception):
        hook._rawdata_sub(m.__rawdata__, Team)
    assert callback_confirm == [
        ({'id': 0, 'status': None, 'team_desc': None, 'team_name': 'OK'},
         Team, 'id', 0)]


def test_rawdata_sub(hook, mocker):
    m = Team(id=0, team_name="hello")

    set_raw = mocker.MagicMock()
    mocker.patch.object(CacheMixinBase, "set_raw", set_raw)
    hook._rawdata_sub(m.__rawdata__, Team)

    set_raw.assert_called_with(
        {'id': 0, 'status': None, 'team_desc': None, "team_name": "hello"})


def test_rollback():
    session = Team._db_session

    try:
        session.add(Team(id=0, team_name="hello"))
        session.flush()
        raise TypeError()
    except:
        session.rollback()
        assert not getattr(session, "pending_write", None)


def test__pub_cache_events_ignore(hook):
    assert hook._pub_cache_events('test_ignore', '') is None


def test_cache_mixin(mocker):
    assert 'team|0' in repr(Team(id=0, team_name="hello"))
    assert Team(id=0, team_name='hello').pk_name() == 'id'
    assert Team(id=0, team_name='hello').pk_attribute().key == 'id'

    t = Team(team_name='test_cache_mixin')
    DBSession.add(t)
    DBSession.commit()
    assert len(Team.mget([t.id], force=True)) == 1

    with pytest.raises(NotImplementedError):
        CacheMixinBase._cache_client.error

    with pytest.raises(NotImplementedError):
        CacheMixinBase._db_session.error

    with mocker.patch.object(CacheMixinBase, 'RAWDATA_VERSION',
                             mocker.MagicMock(__str__=lambda x: '233')):
        assert Team.gen_raw_key(1) == 'team|1|233'

    with mocker.patch.object(Team.__mapper__, 'primary_key',
                             mocker.MagicMock(return_value=[],
                                              __nonzero__=mocker.MagicMock(
                                                  return_value=False))):
        assert Team(id=2, team_name='hello').pk_name() is None
        assert Team(id=3, team_name='hello').pk_attribute() is None


def test_cache_mixin__from_cache_error(mocker):
    with mocker.patch.object(Team._cache_client, 'mget',
                             mocker.MagicMock(
                                     side_effect=ConnectionError)):
        assert Team._from_cache([1]) == {}

    with mocker.patch.object(Team._cache_client, 'mget',
                             mocker.MagicMock(side_effect=TypeError)):
        assert Team._from_cache([2]) == {}


def test_cache_mixin_clear_fail_callback(mocker):

    class A(CacheMixinBase):
        _set_fail_callbacks = set()

    callback = mocker.MagicMock()
    A.register_set_fail_callback(callback)
    assert A._set_fail_callbacks == set([(callback, False)])

    A.clear_set_fail_callbacks()
    assert A._set_fail_callbacks == set()


def test_cache_mixin_fail_callback(mocker):

    class A(CacheMixinBase):
        _set_fail_callbacks = set()

    c1 = mocker.MagicMock()
    c2 = mocker.MagicMock(side_effect=ValueError)
    A.register_set_fail_callback(c1)
    A.register_set_fail_callback(c2, True)

    with pytest.raises(ValueError):
        A._call_set_fail_callbacks('a', 233, 'data')

    A.clear_set_fail_callbacks()
    A.register_set_fail_callback(c2, False)
    A._call_set_fail_callbacks('a', 233, 'data')


def test_cache_mixin_get_error(mocker):
    with mocker.patch.object(Team._cache_client, 'get',
                             mocker.MagicMock(side_effect=ConnectionError)):
        assert Team.get(0) is None

    with mocker.patch.object(Team._cache_client, 'get',
                             mocker.MagicMock(side_effect=TypeError)):
        assert Team.get(0) is None


def test_methods_ignore():
    assert CacheMixinBase.set_raw(None) is None
    assert CacheMixinBase._mset([]) is None


def test_mget_cache_only():
    assert Team.mget_cache_only([]) == []
    assert Team.mget_cache_only([], as_dict=True) == {}

    assert Team.mget_cache_only([233666]) == []
    assert Team.mget_cache_only([233666], as_dict=True) == {}


def test_make_transient_to_detached_error():
    t = Team(id=0, team_name='233')
    with pytest.raises(sa_exc.InvalidRequestError):
        DBSession.add(t)
        make_transient_to_detached(t)
