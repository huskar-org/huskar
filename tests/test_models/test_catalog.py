from __future__ import absolute_import

import json

from pytest import fixture, raises, mark
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN
from marshmallow.exceptions import ValidationError

from huskar_api import settings
from huskar_api.models.catalog import ServiceInfo, ClusterInfo


@fixture
def application_name(faker):
    return faker.uuid4()


@fixture
def service_info(zk, application_name):
    return ServiceInfo(
        zk, type_name=SERVICE_SUBDOMAIN, application_name=application_name)


@fixture
def cluster_info(zk, application_name):
    return ClusterInfo(
        zk, type_name=SERVICE_SUBDOMAIN, application_name=application_name,
        cluster_name='overall')


def test_empty(zk, cluster_info):
    zk.create(cluster_info.path, makepath=True, value=b'null')
    cluster_info.load()
    assert cluster_info.data == {}


def test_get_info(zk, cluster_info):
    zk.create(cluster_info.path, makepath=True, value=json.dumps({
        'route': {'a': 'b'},
        'dependency': {'a': 'b'},
        'info': {
            'protocol': 'Redis'
        }
    }))
    cluster_info.load()
    assert cluster_info.get_info() == {
        'protocol': 'Redis'
    }


def test_set_info(zk, cluster_info):
    zk.create(cluster_info.path, makepath=True, value=json.dumps({
        'route': {'a': 'b'},
        'info': {
            'protocol': 'TCP',
        }
    }))
    cluster_info.load()
    cluster_info.set_info({
        'protocol': 'Redis'
    })
    cluster_info.save()
    assert cluster_info.get_info() == {
        'protocol': 'Redis'
    }

    data, stat = zk.get(cluster_info.path)
    assert cluster_info.stat == stat
    assert json.loads(data) == {
        'route': {'a': 'b'},
        'info': {
            'protocol': 'Redis'
        },
        '_version': '1'
    }


@mark.parametrize('data', [{'base.foo': []}, {'base.foo': ['overall']}],
                  ids=enumerate)
def test_validate_dependency_ok(zk, service_info, data):
    service_info.load()
    service_info.data = {'dependency': data}
    service_info.save()
    remote_data, remote_stat = zk.get(service_info.path)
    assert remote_data == json.dumps({'dependency': data, '_version': '1'})
    assert remote_stat == service_info.stat
    assert remote_stat.version == 0


@mark.parametrize('data,error_text', [
    ({'': []}, 'Invalid application name'),
    ({'base.foo': {}}, 'Invalid cluster list'),
    ({'base.foo': [{}]}, 'Invalid cluster name'),
], ids=[
    'application_name',
    'cluster_list',
    'cluster_name',
])
def test_validate_dependency_error(zk, service_info, data, error_text):
    service_info.load()
    service_info.data = {'dependency': data}
    with raises(ValidationError) as error:
        service_info.save()
    assert error.value.args[0] == {'dependency': [error_text]}
    assert not zk.exists(service_info.path)


def test_get_dependency(zk, service_info):
    service_info.load()
    assert service_info.get_dependency() == {}

    zk.create(
        service_info.path, b'{"dependency":{"base.foo":["s1"]}}',
        makepath=True)
    service_info.load()
    assert service_info.get_dependency() == {'base.foo': ['s1']}


def test_freeze_dependency(zk, service_info):
    service_info.load()
    assert service_info.freeze_dependency() == frozenset()

    zk.create(
        service_info.path, b'{"dependency":{"base.foo":["s1"]}}',
        makepath=True)
    service_info.load()
    frozen_dependency = service_info.freeze_dependency()
    assert frozen_dependency == frozenset([('base.foo', frozenset(['s1']))])


def test_add_dependency(zk, service_info):
    service_info.load()

    service_info.add_dependency('base.foo', 'stable-1')
    service_info.add_dependency('base.foo', 'stable-2')
    service_info.add_dependency('base.bar', 'stable-1')
    assert service_info.data == {'dependency': {
        'base.foo': ['stable-1', 'stable-2'],
        'base.bar': ['stable-1'],
    }}

    service_info.save()

    remote_data, remote_stat = zk.get(service_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data.pop('_version') == '1'
    assert remote_data == service_info.data
    assert remote_stat == service_info.stat
    assert remote_stat.version == 0

    service_info.add_dependency('base.bar', 'stable-1')
    service_info.add_dependency('base.bar', 'stable-2')
    service_info.save()

    remote_data, remote_stat = zk.get(service_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data['dependency'] == {
        'base.foo': ['stable-1', 'stable-2'],
        'base.bar': ['stable-1', 'stable-2'],
    }
    assert remote_stat.version == 1


def test_discard_dependency(zk, service_info):
    service_info.load()
    service_info.discard_dependency('base.foo', 'stable-1')
    assert service_info.data == {'dependency': {'base.foo': []}}
    service_info.save()

    zk.set(service_info.path, b'{"dependency":{"base.foo":["s1"]}}')
    service_info.load()
    assert service_info.data == {'dependency': {'base.foo': ['s1']}}

    service_info.discard_dependency('base.foo', 's2')
    service_info.discard_dependency('base.bar', 's1')
    assert service_info.data == {'dependency': {
        'base.foo': ['s1'], 'base.bar': []}}

    service_info.discard_dependency('base.foo', 's1')
    assert service_info.data == {'dependency': {
        'base.foo': [], 'base.bar': []}}


@mark.parametrize('_data,_ezone,_intent,_result', [
    ({'overall': {'direct': 'stable'}, 'alta1': {'direct': 'xx'}},
     'alta1', 'direct', 'alta1-xx'),
    ({'overall': {'direct': 'stable'}, 'alta1': {'direct': 'xx'}},
     'altb1', 'direct', 'altb1-stable'),
    ({'overall': {'direct': 'stable'}, 'alta1': {'direct': 'xx'}},
     'overall', 'direct', 'stable'),
    ({'overall': {'direct': 'stable'}},
     'overall', 'direct', 'stable'),
    ({}, 'alta1', 'direct', 'alta1-channel-stable-1'),
    ({}, 'overall', 'direct', 'channel-stable-1'),
    ({}, 'overall', None, 'channel-stable-1'),
], ids=lambda x: x[:10])
def test_find_default_route(zk, service_info, _data, _ezone, _intent, _result):
    zk.create(service_info.path, json.dumps({
        'default_route': _data,
    }), makepath=True)
    service_info.load()
    assert service_info.find_default_route(_ezone, _intent) == _result


def test_find_default_route_but_not_find(zk, service_info, mocker):
    zk.create(service_info.path, '{}', makepath=True)
    service_info.load()
    mocker.patch.object(settings, 'ROUTE_DEFAULT_POLICY', {})
    with raises(ValueError):
        assert service_info.find_default_route('overall', 'direct')


@mark.parametrize('_ezone,_intent,_result', [
    ('altb1', None, 'altb1-channel-stable-1'),
    ('overall', 'direct', 'channel-stable-1'),
])
def test_find_global_default_route(_ezone, _intent, _result):
    cluster_name = ServiceInfo.find_global_default_route(_ezone, _intent)
    assert cluster_name == _result


def test_get_default_route(zk, service_info):
    service_info.load()
    assert service_info.get_default_route() == {'overall': {
        'direct': 'channel-stable-1',
    }}

    data = b'{"default_route":{"overall":{"direct": "channel-stable-2"}}}'
    zk.create(service_info.path, data, makepath=True)
    service_info.load()
    assert service_info.get_default_route() == {'overall': {
        'direct': 'channel-stable-2',
    }}


def test_set_default_route(zk, service_info):
    service_info.load()
    service_info.set_default_route('alta1', 'direct', 'channel-stable-2')
    service_info.set_default_route('overall', 'direct', 'channel-stable-3')
    service_info.save()

    remote_data, remote_stat = zk.get(service_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data['default_route'] == {
        'overall': {
            'direct': 'channel-stable-3',
        },
        'alta1': {
            'direct': 'channel-stable-2',
        },
    }
    assert remote_stat == service_info.stat

    with raises(ValueError):
        service_info.set_default_route('alta1', 'direct', '')


@mark.parametrize('ezone,intent,cluster_name,error_text', [
    ('wubba', 'direct', 'channel-stable-1', 'Unexpected ezone'),
    ('alta1', 'lubba', 'channel-stable-1', 'Unexpected intent'),
])
def test_set_default_route_with_invalid_args(
        zk, service_info, ezone, intent, cluster_name, error_text):
    service_info.load()
    with raises(ValueError) as error:
        service_info.set_default_route(ezone, intent, cluster_name)
    assert error.value.args == (error_text,)


def test_discard_default_route(zk, service_info):
    data = json.dumps({'default_route': {
        'overall': {
            'direct': 'channel-stable-3',
        },
        'alta1': {
            'direct': 'channel-stable-2',
        },
    }})
    zk.create(service_info.path, data, makepath=True)

    service_info.load()
    service_info.discard_default_route('overall', 'direct')
    service_info.save()

    remote_data, remote_stat = zk.get(service_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data['default_route'] == {
        'overall': {},
        'alta1': {'direct': 'channel-stable-2'},
    }
    assert remote_stat == service_info.stat


@mark.parametrize('ezone,intent,error_text', [
    ('wubba', 'direct', 'Unexpected ezone'),
    ('alta1', 'lubba', 'Unexpected intent'),
])
def test_discard_default_route_with_invalid_args(
        zk, service_info, ezone, intent, error_text):
    service_info.load()
    with raises(ValueError) as error:
        service_info.discard_default_route(ezone, intent)
    assert error.value.args == (error_text,)


def test_get_route(zk, cluster_info):
    cluster_info.load()
    assert cluster_info.get_route() == {}

    zk.create(cluster_info.path, b'{"route":{"base.foo":"s1"}}', makepath=True)
    cluster_info.load()
    assert cluster_info.get_route() == {'base.foo': 's1'}


def test_set_route(zk, cluster_info):
    cluster_info.load()
    cluster_info.set_route('base.bar', 's1')
    cluster_info.save()

    remote_data, remote_stat = zk.get(cluster_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data == {'route': {'base.bar': 's1'}, '_version': '1'}
    assert remote_stat == cluster_info.stat

    cluster_info.set_route('base.bar', 's2')
    cluster_info.save()

    remote_data, remote_stat = zk.get(cluster_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data == {'route': {'base.bar': 's2'}, '_version': '1'}
    assert remote_stat == cluster_info.stat


def test_discard_route(zk, cluster_info):
    cluster_info.load()
    cluster_info.discard_route('base.bar')
    cluster_info.save()

    remote_data, remote_stat = zk.get(cluster_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data == {'route': {}, '_version': '1'}
    assert remote_stat == cluster_info.stat

    zk.set(cluster_info.path, b'{"route":{"base.baz":"s3"},"_version":"1"}')
    cluster_info.load()
    cluster_info.discard_route('base.baz')
    cluster_info.save()

    remote_data, remote_stat = zk.get(cluster_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data == {'route': {}, '_version': '1'}
    assert remote_stat == cluster_info.stat


def test_get_link(zk, cluster_info):
    cluster_info.load()
    assert cluster_info.get_link() is None

    zk.create(cluster_info.path, b'{"link":["s1"]}', makepath=True)
    cluster_info.load()
    assert cluster_info.get_link() == 's1'

    zk.set(cluster_info.path, b'{"link":["s1", "s2"]}')
    cluster_info.load()
    assert cluster_info.get_link() == 's1+s2'


def test_change_link(zk, cluster_info):
    cluster_info.load()
    cluster_info.set_link('foo')
    cluster_info.save()

    remote_data, remote_stat = zk.get(cluster_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data == {'link': ['foo'], '_version': '1'}
    assert remote_stat == cluster_info.stat

    cluster_info.set_link('s1+s2')
    cluster_info.save()

    remote_data, remote_stat = zk.get(cluster_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data == {'link': ['s1', 's2'], '_version': '1'}
    assert remote_stat == cluster_info.stat

    cluster_info.delete_link()
    cluster_info.save()

    remote_data, remote_stat = zk.get(cluster_info.path)
    remote_data = json.loads(remote_data)
    assert remote_data == {'link': [], '_version': '1'}
    assert remote_stat == cluster_info.stat
