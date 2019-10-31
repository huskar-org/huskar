from __future__ import absolute_import

import functools

from huskar_api.models.tree.common import parse_path, ClusterMap


def test_path():
    p = functools.partial(parse_path, '/huskar')

    path = p('/huskar/service')
    assert not path.is_none()
    assert path.get_level() == 1
    assert path.type_name == 'service'

    path = p('/huskar/service/base.foo')
    assert not path.is_none()
    assert path.get_level() == 2
    assert path.type_name == 'service'
    assert path.application_name == 'base.foo'

    path = p('/huskar/service/base.foo/stable')
    assert not path.is_none()
    assert path.get_level() == 3
    assert path.type_name == 'service'
    assert path.application_name == 'base.foo'
    assert path.cluster_name == 'stable'

    path = p('/huskar/service/base.foo/stable/10.0.0.1_5000')
    assert not path.is_none()
    assert path.get_level() == 4
    assert path.type_name == 'service'
    assert path.application_name == 'base.foo'
    assert path.cluster_name == 'stable'
    assert path.data_name == '10.0.0.1_5000'

    path = p('/huskar/service/base.foo/stable/10.0.0.1_5000/runtime')
    assert path.is_none()

    path = p('/huskar-service')
    assert path.is_none()

    path = p('/')
    assert path.is_none()


def test_cluster_map():
    cluster_map = ClusterMap()

    # Empty-tolerance
    cluster_map.register('foo', None)
    assert cluster_map.cluster_names == {}
    assert cluster_map.resolved_names == {}

    # Empty-tolerance
    cluster_map.deregister('foo')
    assert cluster_map.cluster_names == {}
    assert cluster_map.resolved_names == {}

    # Register symlink or route
    cluster_map.register('foo', 'bar')
    cluster_map.register('baz', 'bar')
    cluster_map.register('s', 'e')
    assert cluster_map.cluster_names == {'foo': 'bar', 'baz': 'bar', 's': 'e'}
    assert cluster_map.resolved_names == {'bar': {'foo', 'baz'}, 'e': {'s'}}

    # Deregister symlink or route
    cluster_map.deregister('baz')
    cluster_map.deregister('s')
    assert cluster_map.cluster_names == {'foo': 'bar'}
    assert cluster_map.resolved_names == {'bar': {'foo'}, 'e': set()}

    # Register multiplex symlink
    cluster_map.register('baz', 'bar+foo')
    assert cluster_map.cluster_names == {'foo': 'bar', 'baz': 'bar+foo'}
    assert cluster_map.resolved_names == {
        'bar': {'foo', 'baz'}, 'foo': {'baz'}, 'e': set()}

    # Deregister multiplex symlink
    cluster_map.deregister('baz')
    assert cluster_map.cluster_names == {'foo': 'bar'}
    assert cluster_map.resolved_names == {
        'bar': {'foo'}, 'foo': set(), 'e': set()}
