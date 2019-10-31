from __future__ import absolute_import

import json
import copy

from pytest import fixture, raises, mark
from marshmallow.exceptions import ValidationError

from huskar_api.models import huskar_client
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.instance.schema import InfraInfo
from huskar_api.models.exceptions import \
    MalformedDataError, NotEmptyError, InfraNameNotExistError
from huskar_api.service.exc import DataNotEmptyError
from huskar_api.service.service import ServiceData


@fixture
def application_name(faker):
    return faker.uuid4()[:8]


@fixture
def instance_management(application_name):
    return InstanceManagement(huskar_client, application_name, 'service')


def test_invalid_choice(application_name):
    instance_management = InstanceManagement(
        huskar_client, application_name, 'config')
    with raises(AssertionError):
        InstanceManagement(huskar_client, application_name, 'woo')
    with raises(AssertionError):
        instance_management.get_service_info()
    with raises(AssertionError):
        instance_management.get_cluster_info('any')


def test_list_cluster_names(zk, application_name, instance_management):
    assert instance_management.list_cluster_names() == []
    zk.create('/huskar/service/%s/foo' % application_name, makepath=True)
    zk.create('/huskar/service/%s/bar' % application_name, makepath=True)
    assert instance_management.list_cluster_names() == ['bar', 'foo']


def test_list_instance_keys(zk, application_name, instance_management):
    assert instance_management.list_instance_keys('foo') == []
    assert instance_management.list_instance_keys('bar') == []
    zk.create('/huskar/service/%s/foo/svc-0' % application_name, makepath=True)
    zk.create('/huskar/service/%s/foo/svc-1' % application_name, makepath=True)
    zk.create('/huskar/service/%s/bar/svc-2' % application_name, makepath=True)
    zk.create('/huskar/service/{0}/slash'.format(application_name))
    zk.create('/huskar/service/{0}/slash/a%SLASH%b'.format(application_name))
    zk.create('/huskar/service/{0}/slash/cd'.format(application_name))
    assert instance_management.list_instance_keys('foo') == ['svc-0', 'svc-1']
    assert instance_management.list_instance_keys('bar') == ['svc-2']
    assert instance_management.list_instance_keys('slash') == ['a/b', 'cd']


def test_list_instance_keys_with_symlink(
        zk, application_name, instance_management):
    zk.create('/huskar/service/%s/bar/svc-0' % application_name, makepath=True)
    zk.create('/huskar/service/%s/bar/svc-1' % application_name, makepath=True)
    assert instance_management.list_instance_keys('foo') == []
    assert instance_management.list_instance_keys('foo', resolve=False) == []
    zk.create('/huskar/service/%s/foo' % application_name, makepath=True,
              value=b'{"link":["bar"]}')
    assert instance_management.list_instance_keys('foo') == ['svc-0', 'svc-1']
    assert instance_management.list_instance_keys('foo', resolve=False) == []
    zk.set('/huskar/service/%s/foo' % application_name, value=b'{"link":[]}')
    assert instance_management.list_instance_keys('foo') == []
    assert instance_management.list_instance_keys('foo', resolve=False) == []


def test_list_instance_keys_without_symlink(
        mocker, zk, application_name, instance_management):
    mocker.patch.object(instance_management, 'type_name', 'switch')
    zk.create('/huskar/switch/%s/bar/abc' % application_name, makepath=True)
    assert instance_management.list_instance_keys('foo') == []
    zk.create('/huskar/switch/%s/foo' % application_name, makepath=True,
              value=b'{"link":["bar"]}')
    assert instance_management.list_instance_keys('foo') == []
    zk.create('/huskar/switch/%s/foo/def' % application_name, makepath=True,
              value=b'{"link":["bar"]}')
    assert instance_management.list_instance_keys('foo') == ['def']


def test_get_service_info(zk, application_name, instance_management):
    info = instance_management.get_service_info()
    assert info.stat is None and info.data is None

    zk.create('/huskar/service/%s' % application_name, makepath=True,
              value=b'{"info": {"protocol":"Redis"}}')

    info = instance_management.get_service_info()
    assert info.stat.version == 0
    assert info.data == {'info': {'protocol': 'Redis'}}


def test_get_service_info_malformed(zk, application_name, instance_management):
    zk.create('/huskar/service/%s' % application_name,
              value=b'xxx', makepath=True)
    with raises(MalformedDataError) as error:
        instance_management.get_service_info()
    assert error.value.info.stat.version == 0
    assert error.value.info.data is None


def test_get_cluster_info(zk, application_name, instance_management):
    info = instance_management.get_cluster_info('foo')
    assert info.stat is None and info.data is None

    zk.create('/huskar/service/%s/foo' % application_name, makepath=True,
              value=b'{"info":{"protocol":"Redis"}}')

    info = instance_management.get_cluster_info('foo')
    assert info.stat.version == 0
    assert info.data == {'info': {'protocol': 'Redis'}}


def test_get_instance(zk, application_name, instance_management):
    info, _ = instance_management.get_instance('stable', 'svc-0')
    assert info.stat is None and info.data is None

    zk.create('/huskar/service/%s/stable/svc-0' % application_name,
              makepath=True, value=b'{"ip":"0.0.0.0","port":{"main":1}}')

    info, _ = instance_management.get_instance('stable', 'svc-0')
    assert info.stat.version == 0
    assert json.loads(info.data) == {'ip': '0.0.0.0', 'port': {'main': 1}}

    zk.create('/huskar/service/{0}/stable/a%SLASH%b'.format(application_name),
              makepath=True, value=b'{"ip":"0.0.0.1","port":{"main":2}}')

    info, _ = instance_management.get_instance('stable', 'a/b')
    assert info.stat.version == 0
    assert json.loads(info.data) == {'ip': '0.0.0.1', 'port': {'main': 2}}


def test_get_instance_with_symlink(zk, application_name, instance_management):
    zk.create('/huskar/service/%s/stable/svc-0' % application_name,
              makepath=True, value=b'{"ip":"0.0.0.0","port":{"main":1}}')
    zk.create('/huskar/service/%s/testing/svc-0' % application_name,
              makepath=True, value=b'{"ip":"0.0.0.1","port":{"main":2}}')

    info, physical_name = instance_management.get_instance('stable', 'svc-0')
    assert info.stat.version == 0
    assert json.loads(info.data) == {'ip': '0.0.0.0', 'port': {'main': 1}}
    assert physical_name is None

    zk.set('/huskar/service/%s/stable' % application_name,
           value=b'{"link":["testing"]}')

    info, physical_name = instance_management.get_instance('stable', 'svc-0')
    assert info.stat.version == 0
    assert json.loads(info.data) == {'ip': '0.0.0.1', 'port': {'main': 2}}
    assert physical_name == 'testing'

    info, physical_name = instance_management.get_instance('stable', 'svc-0',
                                                           resolve=False)
    assert info.stat.version == 0
    assert json.loads(info.data) == {'ip': '0.0.0.0', 'port': {'main': 1}}
    assert physical_name is None

    zk.set('/huskar/service/%s/stable' % application_name,
           value=b'xxx')

    info, physical_name = instance_management.get_instance('stable', 'svc-0')
    assert info.stat.version == 0
    assert json.loads(info.data) == {'ip': '0.0.0.0', 'port': {'main': 1}}
    assert physical_name is None


def test_get_instance_without_symlink(
        mocker, zk, application_name, instance_management):
    mocker.patch.object(instance_management, 'type_name', 'switch')

    zk.create('/huskar/switch/%s/stable/abc' % application_name,
              makepath=True, value=b'100')
    zk.create('/huskar/switch/%s/testing/abc' % application_name,
              makepath=True, value=b'10')

    info, physical_name = instance_management.get_instance('stable', 'abc')
    assert info.stat.version == 0
    assert info.data == '100'
    assert physical_name is None

    zk.set('/huskar/switch/%s/stable' % application_name,
           value=b'{"link":["testing"]}')

    info, physical_name = instance_management.get_instance('stable', 'abc')
    assert info.stat.version == 0
    assert info.data == '100'
    assert physical_name is None


@mark.parametrize('adata,cname,cdata,faname,fcname,default,resolved', [
    ('', 'stable', b'{"link":[]}', None, None, None, None),
    ('', 'direct', b'{"link":[]}', 'foo', 'stable', 'channel-stable-1',
     'channel-stable-1'),
    ('', 'stable', b'broken', None, None, None, None),
    ('', 'direct', b'broken', 'foo', 'stable', 'channel-stable-1',
     'channel-stable-1'),
    ('', 'stable', b'{"link":["testing"]}', None, None, None, 'testing'),
    ('', 'stable', b'{"link":["a", "b"]}', None, None, None, "a+b"),
    ('', 'stable', b'{"link":["testing"], "route":{"foo": "bar"}}', None, None,
     None, 'testing'),
    ('', 'direct', b'{"link":["testing"], "route":{"foo": "bar"}}', 'foo',
     'stable', 'channel-stable-1', 'bar'),
    ('', 'direct', b'{"link":["testing"], "route":{"foo": "stable"}}', 'foo',
     'stable', 'channel-stable-1', 'testing'),
    ('', 'direct', b'{"link":["testing"], "route":{"foo": "stage"}}', 'foo',
     'stable', 'channel-stable-1', 'stage0'),
    ('', 'stable', b'{"route":{}}', None, None, None, None),
    ('', 'direct', b'{"route":{}}', 'foo', 'stable', 'channel-stable-1',
     'channel-stable-1'),
    ('', 'direct', b'{"route":{}}', 'foo', 'alta1-stable',
     'alta1-channel-stable-1', 'alta1-channel-stable-1'),
    ('{"default_route":{"altb1":{"direct":"1"}}}', 'direct', b'{"route":{}}',
     'foo', 'alta1-stable', 'alta1-channel-stable-1',
     'alta1-channel-stable-1'),
    ('{"default_route":{"altb1":{"direct":"1"}}}', 'direct', b'{"route":{}}',
     'foo', 'altb1-stable', 'altb1-channel-stable-1', 'altb1-1'),
    ('{', 'direct', b'{"route":{}}', 'foo', 'altb1-stable',
     'altb1-channel-stable-1', 'altb1-channel-stable-1'),
])
def test_resolve_cluster_name(
        mocker, zk, application_name, instance_management,
        adata, cname, cdata, faname, fcname, default, resolved):
    instance_management.set_context(faname, fcname)
    resolve = instance_management.resolve_cluster_name

    # misleading test
    zk.create('/huskar/service/%s/stage' % application_name, makepath=True,
              value=b'{"link":["stage0"], "route": {"foo": "stable"}}')

    # resolve on service cluster
    mocker.patch.object(instance_management, 'type_name', 'service')
    assert resolve(cname) == default
    zk.set('/huskar/service/%s' % application_name, value=adata)
    zk.create('/huskar/service/%s/stable' % application_name, makepath=True,
              value=cdata)
    assert resolve(cname) == resolved

    # resolve on switch cluster
    mocker.patch.object(instance_management, 'type_name', 'switch')
    assert resolve(cname) is None
    zk.create('/huskar/switch/%s/stable' % application_name, makepath=True,
              value=b'{"link":["testing"]}')
    assert resolve(cname) is None


def test_delete_cluster(zk, application_name, instance_management):
    zk.create('/huskar/service/%s' % application_name, makepath=True,
              value=b'{"dependency":{"base.foo":["c6"]}}')
    zk.create('/huskar/service/%s/bar' % application_name, makepath=True)
    zk.create('/huskar/service/%s/baz' % application_name, makepath=True,
              value=b'{"route":{},"link":[],"_version":"1"}')
    zk.create('/huskar/service/%s/c1' % application_name, makepath=True,
              value=b'{"route":{"a": "b"}}')
    zk.create('/huskar/service/%s/c2' % application_name, makepath=True,
              value=b'{"link":["a"]}')
    zk.create('/huskar/service/%s/c3' % application_name, makepath=True,
              value=b'{"info":{"protocol":"TCP"}}')
    zk.create('/huskar/service/%s/c4/10' % application_name, makepath=True)

    assert instance_management.delete_cluster('foo') is None
    assert instance_management.delete_cluster('bar').stat is None
    assert not zk.exists('/huskar/service/%s/foo' % application_name)
    assert not zk.exists('/huskar/service/%s/bar' % application_name)

    for cluster_name in ('c1', 'c2', 'c3', 'c4'):
        with raises(NotEmptyError):
            instance_management.delete_cluster(cluster_name)

        with raises(DataNotEmptyError):
            ServiceData.delete_cluster(
                    application_name, cluster_name, strict=True)
        ServiceData.delete_cluster(
                application_name, cluster_name, strict=False)


@mark.xparametrize
def test_infra_info_ok(zk, application_name, _data, _type):
    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.data = copy.deepcopy(_data)
    schema.save()

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data == _data
    assert schema.stat == zk.exists(schema.path)


@mark.xparametrize
def test_infra_info_fail(application_name, _data, _type, _error):
    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.data = copy.deepcopy(_data)
    with raises(ValidationError) as error:
        schema.save()
    error.match(_error)

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data != _data


@mark.xparametrize
def test_infra_info_list(application_name, _data, _type, _name, _result):
    if _data:
        schema = InfraInfo(huskar_client.client, application_name, _type)
        schema.data = copy.deepcopy(_data)
        schema.save()

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data == _data
    assert schema.list_by_infra_name(_name) == list(map(tuple, _result))


@mark.xparametrize
def test_infra_info_get(application_name, _data, _type, _name, _args, _result):
    if _data:
        schema = InfraInfo(huskar_client.client, application_name, _type)
        schema.data = copy.deepcopy(_data)
        schema.save()

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data == _data
    assert schema.get_by_name(
        _name, **_args) == {'url': 'sam+redis://redis.10010/overall.alta'}


@mark.xparametrize
def test_infra_info_set(application_name, _data, _type, _name, _args, _result):
    if _data:
        schema = InfraInfo(huskar_client.client, application_name, _type)
        schema.data = copy.deepcopy(_data)
        schema.save()

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data == _data
    for value in [None, '', 233, []]:
        with raises(ValueError):
            _invalid_args = dict(_args)
            _invalid_args['value'] = value
            schema.set_by_name(_name, **_invalid_args)
    schema.set_by_name(_name, **_args)
    assert schema.data == _result
    schema.save()

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data == _result


@mark.xparametrize
def test_infra_info_update(application_name,
                           _data, _type, _name, _args, _result):
    if _data:
        schema = InfraInfo(huskar_client.client, application_name, _type)
        schema.data = copy.deepcopy(_data)
        schema.save()

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data == _data
    for value in [None, '', 233, []]:
        with raises(ValueError):
            _invalid_args = dict(_args)
            _invalid_args['value'] = value
            schema.update_by_name(_name, **_invalid_args)
    with raises(InfraNameNotExistError):
        _invalid_name = '123'
        schema.update_by_name(_invalid_name, **_args)
    schema.update_by_name(_name, **_args)
    assert schema.data == _result
    schema.save()

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data == _result


@mark.xparametrize
def test_infra_info_delete(
        application_name, _data, _type, _name, _args, _result):
    if _data:
        schema = InfraInfo(huskar_client.client, application_name, _type)
        schema.data = copy.deepcopy(_data)
        schema.save()

    schema = InfraInfo(huskar_client.client, application_name, _type)
    schema.load()
    assert schema.data == _data
    schema.delete_by_name(_name, **_args)
    assert schema.data == _result


@mark.xparametrize
def test_infra_info_extract_urls(application_name, _type, _value, _result):
    dict_result = {r['key']: r['url'] for r in _result}
    list_result = [r['url'] for r in _result]
    schema = InfraInfo(huskar_client.client, application_name, _type)
    assert schema.extract_urls(_value) == list_result
    assert schema.extract_urls(_value, as_dict=True) == dict_result


def test_create_exist_cluster_not_error(application_name):
    ServiceData.create_cluster(application_name, 'test_233', strict=False)
    ServiceData.create_cluster(application_name, 'test_233', strict=False)


def test_delete_nonempty_cluster_not_error(zk, application_name):
    zk.create('/huskar/service/%s/c1' % application_name, makepath=True,
              value=b'{"protocol":"TCP"}')
    ServiceData.delete_cluster(application_name, 'c1', strict=False)
