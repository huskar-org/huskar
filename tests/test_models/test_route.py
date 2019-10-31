from __future__ import absolute_import

from pytest import fixture, mark, raises

from huskar_api.models import huskar_client
from huskar_api.models.exceptions import EmptyClusterError
from huskar_api.models.route import RouteManagement
from huskar_api.models.route.utils import make_route_key, parse_route_key


@fixture
def route_management(faker):
    return RouteManagement(
        huskar_client=huskar_client,
        application_name=faker.uuid4()[:8],
        cluster_name='altb1-channel-stable-1',
    )


@mark.parametrize('application_name,intent,expected_route_key', [
    ('base.foo', None, 'base.foo'),
    ('base.foo', '', 'base.foo'),
    ('base.foo', 'direct', 'base.foo'),
])
def test_make_route_key(application_name, intent, expected_route_key):
    route_key = make_route_key(application_name, intent)
    assert route_key == expected_route_key


@mark.parametrize('route_key,application_name,intent', [
    ('base.foo', 'base.foo', 'direct'),
    ('base.foo@bar@baz', 'base.foo', 'bar@baz'),
])
def test_parse_route_key(route_key, application_name, intent):
    parsed_route_key = parse_route_key(route_key)
    assert parsed_route_key.application_name == application_name
    assert parsed_route_key.intent == intent


def test_list_route_empty(route_management):
    route = sorted(route_management.list_route())
    assert route == []

    service_info = route_management.make_service_info()
    service_info.add_dependency('base.foo', 'a-unknown-cluster')
    service_info.save()

    route = sorted(route_management.list_route())
    assert route == [], 'unknown clusters should be excluded'

    service_info = route_management.make_service_info()
    service_info.add_dependency('base.foo', route_management.cluster_name)
    service_info.save()

    assert sorted(route_management.list_route()) == [
        ('base.foo', 'direct', None),
    ]


def test_list_route(route_management):
    service_info = route_management.make_service_info()
    service_info.add_dependency('base.foo', 'a-unknown-cluster')
    service_info.add_dependency('base.foo', route_management.cluster_name)
    service_info.save()

    cluster_info = route_management.make_cluster_info('base.foo')
    cluster_info.set_route(route_management.application_name, 's1')
    cluster_info.set_route('base.bar', 's2')
    cluster_info.save()

    assert sorted(route_management.list_route()) == [
        ('base.foo', 'direct', 's1'),
    ]

    cluster_info = route_management.make_cluster_info('base.foo')
    cluster_info.set_route('%s@one' % route_management.application_name, 'w1')
    cluster_info.set_route('base.bar@one', 'w2')
    cluster_info.save()

    assert sorted(route_management.list_route()) == [
        ('base.foo', 'direct', 's1'),
        ('base.foo', 'one', 'w1'),
    ]


def test_set_route(route_management, zk):
    prefix = route_management.application_name

    zk.ensure_path('/huskar/service/%s/%s/foo' % (prefix + '.foo', 's1'))
    route_management.set_route(prefix + '.foo', 's1')
    zk.ensure_path('/huskar/service/%s/%s/foo' % (prefix + '.bar', 's2'))
    route_management.set_route(prefix + '.bar', 's2')
    zk.ensure_path('/huskar/service/%s/%s/foo' % (prefix + '.foo', 'w1'))

    service_info = route_management.make_service_info()
    assert service_info.get_dependency() == {
        prefix + '.foo': [route_management.cluster_name],
        prefix + '.bar': [route_management.cluster_name],
    }

    cluster_info = route_management.make_cluster_info(prefix + '.foo')
    assert cluster_info.get_route() == {
        route_management.application_name: 's1',
    }

    cluster_info = route_management.make_cluster_info(prefix + '.bar')
    assert cluster_info.get_route() == {
        route_management.application_name: 's2',
    }


def test_set_route_with_empty_cluster(route_management, zk):
    dest_application = '%s.foo' % route_management.application_name
    dest_cluster_name = 'bar'
    # not exist
    with raises(EmptyClusterError):
        route_management.set_route(dest_application, dest_cluster_name)

    # no instance
    path = '/huskar/service/%s/%s' % (dest_application, dest_cluster_name)
    zk.ensure_path(path)
    with raises(EmptyClusterError):
        route_management.set_route(dest_application, dest_cluster_name)

    with raises(ValueError):
        route_management.set_route(dest_application, '')


def test_set_route_in_malformed_node(zk, route_management):
    prefix = route_management.application_name

    service_info = route_management.make_service_info()
    service_info.save()
    zk.set(service_info.path, b'{"malformed')

    zk.ensure_path('/huskar/service/%s/%s/foo' % (prefix + '.foo', 's1'))
    route_management.set_route(prefix + '.foo', 's1')
    service_info = route_management.make_service_info()
    assert service_info.get_dependency() == {
        prefix + '.foo': [route_management.cluster_name],
    }

    cluster_info = route_management.make_cluster_info(prefix + '.foo')
    assert cluster_info.get_route() == {
        route_management.application_name: 's1',
    }


def test_discard_route(route_management):
    prefix = route_management.application_name

    service_info = route_management.make_service_info()
    service_info.add_dependency(prefix + '.foo', route_management.cluster_name)
    service_info.add_dependency(prefix + '.bar', route_management.cluster_name)
    service_info.save()

    cluster_info = route_management.make_cluster_info(prefix + '.foo')
    cluster_info.set_route(route_management.application_name, 's1')
    cluster_info.set_route(route_management.application_name + '@one', 'w1')
    cluster_info.save()

    cluster_info = route_management.make_cluster_info(prefix + '.bar')
    cluster_info.set_route(route_management.application_name, 's2')
    cluster_info.save()

    route_management.discard_route(prefix + '.foo', intent='unknown')
    service_info = route_management.make_service_info()
    cluster_info = route_management.make_cluster_info(prefix + '.foo')
    assert service_info.get_dependency() == {
        prefix + '.foo': [route_management.cluster_name],
        prefix + '.bar': [route_management.cluster_name],
    }
    assert cluster_info.get_route() == {
        route_management.application_name: 's1',
        route_management.application_name + '@one': 'w1',
    }

    route_management.discard_route(prefix + '.foo')
    service_info = route_management.make_service_info()
    cluster_info = route_management.make_cluster_info(prefix + '.foo')
    assert service_info.get_dependency() == {
        prefix + '.foo': [route_management.cluster_name],
        prefix + '.bar': [route_management.cluster_name],
    }
    assert cluster_info.get_route() == {
        route_management.application_name + '@one': 'w1',
    }

    route_management.discard_route(prefix + '.foo', intent='one')
    service_info = route_management.make_service_info()
    cluster_info = route_management.make_cluster_info(prefix + '.foo')
    assert service_info.get_dependency() == {
        prefix + '.foo': [],
        prefix + '.bar': [route_management.cluster_name],
    }
    assert cluster_info.get_route() == {}

    route_management.discard_route(prefix + '.bar')
    service_info = route_management.make_service_info()
    cluster_info = route_management.make_cluster_info(prefix + '.bar')
    assert service_info.get_dependency() == {
        prefix + '.foo': [],
        prefix + '.bar': [],
    }
    assert cluster_info.get_route() == {}


def test_declare_upstream(route_management):
    route_management.declare_upstream(['base.foo', 'base.bar'])
    service_info = route_management.make_service_info()
    assert service_info.get_dependency() == {
        'base.foo': [route_management.cluster_name],
        'base.bar': [route_management.cluster_name],
    }
    assert service_info.stat.version == 0

    route_management.declare_upstream(['base.foo'])
    service_info = route_management.make_service_info()
    assert service_info.get_dependency() == {
        'base.foo': [route_management.cluster_name],
        'base.bar': [route_management.cluster_name],
    }
    assert service_info.stat.version == 0

    route_management.declare_upstream(['base.baz'])
    service_info = route_management.make_service_info()
    assert service_info.get_dependency() == {
        'base.foo': [route_management.cluster_name],
        'base.bar': [route_management.cluster_name],
        'base.baz': [route_management.cluster_name],
    }
    assert service_info.stat.version == 1


def test_default_route(route_management):
    assert route_management.get_default_route() == {'overall': {
        'direct': 'channel-stable-1',
    }}

    route_management.set_default_route(
        ezone='alta1', intent=None, cluster_name='channel-stable-2')
    assert route_management.get_default_route() == {'overall': {
        'direct': 'channel-stable-1',
    }, 'alta1': {
        'direct': 'channel-stable-2',
    }}

    assert route_management.get_default_route() == {'overall': {
        'direct': 'channel-stable-1',
    }, 'alta1': {
        'direct': 'channel-stable-2',
    }}

    route_management.discard_default_route(ezone='alta1', intent='direct')
    assert route_management.get_default_route() == {'overall': {
        'direct': 'channel-stable-1',
    }, 'alta1': {}}
