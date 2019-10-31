from __future__ import absolute_import

from pytest import fixture, raises
from gevent import sleep
from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError

from huskar_api.models.znode import ZnodeModel, ZnodeList
from huskar_api.models.exceptions import MalformedDataError, OutOfSyncError


class TestSchema(object):
    def dumps(self, data):
        return data, None

    def loads(self, data):
        return data, None


@fixture
def base_path(zk, faker):
    base_path = '/huskar/test_%s' % faker.uuid4()[:8]
    zk.ensure_path(base_path)
    try:
        yield base_path
    finally:
        zk.delete(base_path, recursive=True)


@fixture
def schema(mocker):
    schema = TestSchema()
    mocker.spy(schema, 'dumps')
    mocker.spy(schema, 'loads')
    return schema


@fixture
def model_class(schema):
    class TestModel(ZnodeModel):
        PATH_PATTERN = '/huskar/service/{application_name}/overall'
        MARSHMALLOW_SCHEMA = schema
    return TestModel


def test_znode_model_injection_attacked(zk, model_class):
    with raises(ValueError):
        model_class(zk, application_name='foo/bar')


def test_znode_model_setdefault(zk, faker, model_class):
    name = faker.uuid4()
    model = model_class(zk, application_name=name)
    assert model.data is None

    will_be_used = {}
    data = model.setdefault(will_be_used)
    data['foo'] = 'bar'
    assert model.data['foo'] == 'bar'
    assert will_be_used['foo'] == 'bar'

    wont_be_used = {}
    data = model.setdefault(wont_be_used)
    data['foo'] = 'baz'
    assert model.data['foo'] == 'baz'
    assert 'foo' not in wont_be_used


def test_znode_model_load_ok(zk, faker, mocker, schema, model_class):
    name = faker.uuid4()
    zk.create('/huskar/service/%s/overall' % name, b'1s', makepath=True)

    model = model_class(zk, application_name=name)
    model.load()
    assert model.data == '1s'
    assert model.stat.version == 0
    assert schema.loads.mock_calls == [mocker.call('1s')]
    assert schema.dumps.mock_calls == []


def test_znode_model_load_nonode(zk, faker, schema, model_class):
    name = faker.uuid4()
    model = model_class(zk, application_name=name)
    model.load()
    assert model.data is None
    assert model.stat is None
    assert schema.loads.mock_calls == []
    assert schema.dumps.mock_calls == []


def test_znode_model_load_malformed(zk, faker, schema, model_class):
    name = faker.uuid4()
    zk.create('/huskar/service/%s/overall' % name, b'1s', makepath=True)
    schema.loads.side_effect = [ValueError]

    model = model_class(zk, application_name=name)
    with raises(MalformedDataError) as error:
        model.load()
    assert error.value.info is model
    assert model.data is None
    assert model.stat.version == 0


def test_znode_model_create(zk, faker, mocker, schema, model_class):
    name = faker.uuid4()
    model = model_class(zk, application_name=name)
    assert model.stat is None
    model.data = '{}'
    model.save()
    assert schema.dumps.mock_calls == [mocker.call('{}')]
    assert schema.loads.mock_calls == [mocker.call('{}'), mocker.call('{}')]
    assert model.stat.version == 0


def test_znode_model_create_oos(zk, faker, mocker, schema, model_class):
    name = faker.uuid4()
    model = model_class(zk, application_name=name)
    model.data = '{}'

    zk.create('/huskar/service/%s/overall' % name, b'1s', makepath=True)
    with raises(OutOfSyncError) as error:
        model.save()
    assert isinstance(error.value.args[0], NodeExistsError)
    assert model.stat is None
    assert model.data == '{}'


def test_znode_model_update(zk, faker, mocker, schema, model_class):
    name = faker.uuid4()
    zk.create('/huskar/service/%s/overall' % name, b'1s', makepath=True)

    model = model_class(zk, application_name=name)
    model.load()
    model.data = '+1s'
    model.save()
    assert schema.dumps.mock_calls == [mocker.call('+1s')]
    assert schema.loads.mock_calls == [mocker.call('1s'), mocker.call('+1s')]
    assert model.stat.version == 1

    data, stat = zk.get('/huskar/service/%s/overall' % name)
    assert data == b'+1s'
    assert stat == model.stat


def test_znode_model_update_oos(zk, faker, model_class):
    name = faker.uuid4()
    zk.create('/huskar/service/%s/overall' % name, b'1s', makepath=True)

    model = model_class(zk, application_name=name)
    model.load()

    zk.set('/huskar/service/%s/overall' % name, b'-1s')
    model.data = '+1s'

    with raises(OutOfSyncError) as error:
        model.save()
    assert model.stat.version == 0
    assert isinstance(error.value.args[0], BadVersionError)

    model.save(version=1)
    assert model.stat.version == 2

    data, stat = zk.get('/huskar/service/%s/overall' % name)
    assert data == b'+1s'
    assert stat == model.stat

    zk.delete(model.path)
    with raises(OutOfSyncError) as error:
        model.save()
    assert isinstance(error.value.args[0], NoNodeError)


def test_znode_model_delete(zk, faker, model_class):
    name = faker.uuid4()
    zk.create('/huskar/service/%s/overall' % name, b'1s', makepath=True)

    model = model_class(zk, application_name=name)
    with raises(OutOfSyncError) as error:
        model.delete()
    assert error.value.args == ()

    model.load()
    model.delete()

    assert not zk.exists('/huskar/service/%s/overall' % name)
    assert model.stat is None
    assert model.data is None


def test_znode_model_delete_oos(zk, faker, model_class):
    name = faker.uuid4()
    zk.create('/huskar/service/%s/overall' % name, b'1s', makepath=True)

    model = model_class(zk, application_name=name)
    model.load()

    zk.set(model.path, b'2s')
    with raises(OutOfSyncError) as error:
        model.delete()
    assert isinstance(error.value.args[0], BadVersionError)
    assert model.stat.version == 0
    assert model.data == b'1s'

    with raises(OutOfSyncError) as error:
        model.delete(version=233)
    assert isinstance(error.value.args[0], BadVersionError)
    assert model.stat.version == 0
    assert model.data == b'1s'


def test_znode_list_provision(zk, base_path):
    zk.ensure_path(base_path + '/foo')
    zk.ensure_path(base_path + '/bar')
    zk.ensure_path(base_path + '/baz')

    znode_list = ZnodeList(zk, base_path)
    znode_list.start()

    sleep(0.1)
    assert znode_list.children == frozenset(['foo', 'bar', 'baz'])


def test_znode_list_synchronize(zk, base_path):
    znode_list = ZnodeList(zk, base_path)
    znode_list.start()

    sleep(0.1)
    assert znode_list.children == frozenset([])

    zk.ensure_path(base_path + '/foo')
    zk.ensure_path(base_path + '/bar')

    sleep(0.1)
    assert znode_list.children == frozenset(['foo', 'bar'])

    zk.delete(base_path + '/bar', recursive=True)

    sleep(0.1)
    assert znode_list.children == frozenset(['foo'])

    # ignore update content
    zk.set(base_path, b'test')
    sleep(0.1)
    assert znode_list.children == frozenset(['foo'])


def test_znode_list_restart(zk, base_path):
    zk.ensure_path(base_path + '/foo')
    zk.ensure_path(base_path + '/bar')

    znode_list = ZnodeList(zk, base_path)
    assert 'ZnodeList' in repr(znode_list)
    znode_list.start()

    sleep(0.1)
    assert znode_list.children == frozenset(['foo', 'bar'])

    zk.delete(base_path, recursive=True)

    sleep(0.1)
    assert znode_list.children == frozenset([])

    zk.ensure_path(base_path + '/baz')

    sleep(0.1)
    assert znode_list.children == frozenset(['baz'])


def test_znode_list_start_twice(zk, base_path, mocker):
    mocker.spy(zk, 'DataWatch')
    mocker.spy(zk, 'ChildrenWatch')

    znode_list = ZnodeList(zk, base_path)
    znode_list.start()
    znode_list.start()

    assert zk.DataWatch.call_count == 1
    assert zk.ChildrenWatch.call_count == 1


def test_znode_list_on_update(mocker, zk, base_path):
    zk.ensure_path(base_path + '/foo')
    zk.ensure_path(base_path + '/bar')

    on_update = mocker.stub()
    znode_list = ZnodeList(zk, base_path, on_update)
    znode_list.start()

    sleep(0.1)
    on_update.assert_called_once_with(frozenset(['foo', 'bar']))

    on_update.reset_mock()
    zk.ensure_path(base_path + '/baz')

    sleep(0.1)
    on_update.assert_called_once_with(frozenset(['foo', 'bar', 'baz']))
