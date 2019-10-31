from __future__ import absolute_import

import json

from pytest import fixture, mark, raises

from huskar_api.models import huskar_client
from huskar_api.models.auth import Application
from huskar_api.models.exceptions import OutOfSyncError
from huskar_api.models.route import RouteManagement
from ..utils import assert_response_ok, assert_response_status_code


@fixture
def route_management(test_application):
    return RouteManagement(
        huskar_client, test_application.application_name, None)


@fixture
def setup_tree(zk, test_application):
    def setup(tree):
        kwargs = {'test_application_name': test_application.application_name}
        for node in tree:
            zk.ensure_path(node['path'] % kwargs)
            zk.set(node['path'] % kwargs, node['data'] % kwargs)
    return setup


@fixture
def check_tree(zk, test_application):
    def check(tree):
        kwargs = {'test_application_name': test_application.application_name}
        for node in tree:
            data, _ = zk.get(node['path'] % kwargs)
            assert json.loads(data) == json.loads(node['data'] % kwargs)
    return check


@fixture(params=[True, False])
def make_token(request, test_application_token, test_team, secret_key):
    def make(data):
        peer = Application.create(data['application_name'], test_team.id)
        if request.param:
            return peer.setup_default_auth().generate_token(secret_key)
        return test_application_token
    return make


@mark.xparametrize
def test_get_route(
        client, zk, test_application, test_token, setup_tree, tree, route):
    setup_tree(tree)
    url = '/api/serviceroute/%s/stable' % test_application.application_name
    r = client.get(url, headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.json['data'] == {'route': route}


@mark.xparametrize
def test_set_route(
        client, zk, test_application, test_application_token, make_token,
        setup_tree, check_tree, last_audit_log, tree,
        dest, result, mocker):
    setup_tree(tree)
    mocker.patch(
        'huskar_api.settings.FORCE_ROUTING_CLUSTERS',
        {'altc1-test-pre': 'altc1-test-pre', 'test-pre': 'test-pre'}
    )
    zk.ensure_path('/huskar/service/%s/%s/%s' % (
        dest['application_name'], dest['cluster_name'], 'foo'))
    url = '/api/serviceroute/%s/stable/%s' % (
        test_application.application_name, dest['application_name'])
    data = {'cluster_name': dest['cluster_name']}
    if 'intent' in dest:
        data['intent'] = dest['intent']
    token = make_token(dest)
    r = client.put(url, data=data, headers={'Authorization': token})
    assert_response_ok(r)
    check_tree(result)

    audit_log = last_audit_log()
    assert audit_log.action_name == 'UPDATE_ROUTE'
    assert audit_log.action_json['application_name'] == \
        test_application.application_name
    assert audit_log.action_json['cluster_name'] == 'stable'
    assert audit_log.action_json['intent'] == dest.get('intent', 'direct')
    url = '/api/serviceroute/%s/altc1-test-pre/%s' % (
        test_application.application_name, dest['application_name'])
    data = {'cluster_name': dest['cluster_name']}
    if 'intent' in dest:
        data['intent'] = dest['intent']
    r = client.put(url, data=data, headers={'Authorization': token})
    assert_response_status_code(r, 403)


def test_set_route_noauth(client, zk, test_application, test_team, test_token):
    Application.create('base.foo', test_team.id)
    url = '/api/serviceroute/%s/stable/base.foo' % (
        test_application.application_name)
    r = client.put(
        url, data={'cluster_name': 'channel-stable-1'},
        headers={'Authorization': test_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'NoAuthError'


def test_set_route_badintent(
        client, zk, test_team, test_application, test_application_token):
    Application.create('base.foo', test_team.id)
    url = '/api/serviceroute/%s/stable/base.foo' % (
        test_application.application_name)
    r = client.put(
        url, data={'cluster_name': 'channel-stable-1', 'intent': 'freestyle'},
        headers={'Authorization': test_application_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'intent must be one of direct'


@mark.parametrize('exist', [True, False])
def test_set_route_failed_dest_empty_cluster(
        client, zk, test_team, test_application,
        test_application_token, exist):
    dest_application_name = '%s_dest' % test_application.application_name
    Application.create(dest_application_name, test_team.id)
    dest_cluster_name = 'channel-stable-1'
    if exist:
        zk.ensure_path('/huskar/service/%s/%s' % (
            dest_application_name, dest_cluster_name))

    url = '/api/serviceroute/%s/stable/%s' % (
        test_application.application_name, dest_application_name)
    r = client.put(
        url, data={'cluster_name': dest_cluster_name, 'intent': 'direct'},
        headers={'Authorization': test_application_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == \
        'The target cluster %s is empty.' % dest_cluster_name


def test_set_route_with_dest_linked_cluster(
        client, zk, test_team, test_application, check_tree,
        test_application_token):
    dest_application_name = '%s_dest' % test_application.application_name
    Application.create(dest_application_name, test_team.id)
    test_application_name = test_application.application_name
    cluster_name = 'stable'
    dest_cluster_name = 'channel-stable-1'
    physical_cluster = 'foo.bar'
    path = '/huskar/service/%s/%s' % (dest_application_name, dest_cluster_name)
    data = json.dumps({'link': [physical_cluster]})
    zk.create(path, data, makepath=True)
    zk.ensure_path('/huskar/service/%s/%s' % (
        test_application_name, cluster_name))
    zk.ensure_path('/huskar/service/%s/%s' % (
        dest_application_name, physical_cluster))
    zk.ensure_path('/huskar/service/%s/%s/key' % (
        dest_application_name, physical_cluster))

    url = '/api/serviceroute/%s/%s/%s' % (
        test_application_name, cluster_name, dest_application_name)
    r = client.put(
        url, data={'cluster_name': dest_cluster_name, 'intent': 'direct'},
        headers={'Authorization': test_application_token})
    assert_response_ok(r)
    result = [
        {
            'path': '/huskar/service/%s' % test_application_name,
            'data': '{"dependency":{"%s":["%s"]},"_version":"1"}' % (
                dest_application_name, cluster_name),
        },
        {
            'path': '/huskar/service/%s/%s' % (
                dest_application_name, cluster_name),
            'data': '{"route":{"%s":"%s"},"_version":"1"}' % (
                test_application_name, dest_cluster_name),
        },
    ]
    check_tree(result)


@mark.parametrize('exist', [True, False])
def test_set_route_failed_dest_linked_empty_cluster(
        client, zk, test_team, test_application,
        test_application_token, exist):
    dest_application_name = '%s_dest' % test_application.application_name
    Application.create(dest_application_name, test_team.id)
    test_application_name = test_application.application_name
    cluster_name = 'stable'
    dest_cluster_name = 'channel-stable-1'
    physical_cluster = 'foo.bar'
    path = '/huskar/service/%s/%s' % (dest_application_name, dest_cluster_name)
    data = json.dumps({'link': [physical_cluster]})
    zk.create(path, data, makepath=True)
    zk.ensure_path('/huskar/service/%s/%s' % (
        test_application_name, cluster_name))
    if exist:
        zk.ensure_path('/huskar/service/%s/%s' % (
            dest_application_name, physical_cluster))

    url = '/api/serviceroute/%s/%s/%s' % (
        test_application_name, cluster_name, dest_application_name)
    r = client.put(
        url, data={'cluster_name': dest_cluster_name, 'intent': 'direct'},
        headers={'Authorization': test_application_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == \
        'The target cluster %s is empty.' % dest_cluster_name


@mark.xparametrize
def test_delete_route(
        client, zk, test_application, test_application_token, make_token,
        setup_tree, check_tree, last_audit_log, tree, deleting, result):
    setup_tree(tree)
    url = '/api/serviceroute/%s/stable/%s' % (
        test_application.application_name, deleting['application_name'])
    data = {'intent': deleting['intent']} if 'intent' in deleting else {}
    r = client.delete(
        url, data=data, headers={'Authorization': make_token(deleting)})
    assert_response_ok(r)
    check_tree(result)

    audit_log = last_audit_log()
    assert audit_log.action_name == 'DELETE_ROUTE'
    assert audit_log.action_json['application_name'] == \
        test_application.application_name
    assert audit_log.action_json['cluster_name'] == 'stable'
    assert audit_log.action_json['intent'] == deleting.get('intent', 'direct')


def test_delete_route_noauth(
        client, zk, test_application, test_team, test_token):
    Application.create('base.foo', test_team.id)
    url = '/api/serviceroute/%s/stable/base.foo' % (
        test_application.application_name)
    r = client.delete(url, headers={'Authorization': test_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'NoAuthError'


@mark.xparametrize
def test_get_default_route(
        client, zk, test_application, test_token, setup_tree, tree, result):
    setup_tree(tree)
    url = '/api/serviceroute/default/%s' % test_application.application_name
    r = client.get(url, headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.json['data'] == result


@mark.xparametrize
def test_set_default_route(
        client, zk, test_application, test_application_token, route_management,
        setup_tree, last_audit_log, tree, data, result):
    setup_tree(tree)
    url = '/api/serviceroute/default/%s' % test_application.application_name
    headers = {'Authorization': test_application_token}
    r = client.put(url, data=data, headers=headers)
    present = route_management.get_default_route()
    audit_log = last_audit_log()
    assert r.json == result
    if r.json['status'] == 'SUCCESS':
        assert r.json['data']['default_route'] == present
        assert audit_log.action_name == 'UPDATE_DEFAULT_ROUTE'
        assert audit_log.action_json['application_name'] == \
            test_application.application_name
        assert audit_log.action_json['ezone'] == data.get('ezone', 'overall')
        assert audit_log.action_json['intent'] == data.get('intent', 'direct')
        assert audit_log.action_json['cluster_name'] == data['cluster_name']
    else:
        assert audit_log is None


@mark.xparametrize
def test_discard_default_route(
        client, zk, test_application, test_application_token, route_management,
        setup_tree, last_audit_log, tree, data, result):
    setup_tree(tree)
    url = '/api/serviceroute/default/%s' % test_application.application_name
    headers = {'Authorization': test_application_token}
    r = client.delete(url, data=data, headers=headers)
    present = route_management.get_default_route()
    audit_log = last_audit_log()
    assert r.json == result
    if r.json['status'] == 'SUCCESS':
        assert r.json['data']['default_route'] == present
        assert audit_log.action_name == 'DELETE_DEFAULT_ROUTE'
        assert audit_log.action_json['application_name'] == \
            test_application.application_name
        assert audit_log.action_json['ezone'] == data.get('ezone', 'overall')
        assert audit_log.action_json['intent'] == data.get('intent', 'direct')
    else:
        assert audit_log is None


def test_set_default_route_outofsyncerror(
        client, zk, test_application, test_application_token, mocker):
    url = '/api/serviceroute/default/%s' % test_application.application_name
    headers = {'Authorization': test_application_token}
    mocked_route_management = mocker.MagicMock()
    mocker.patch('huskar_api.api.service_route.RouteManagement',
                 return_value=mocked_route_management)
    mocked_route_management.set_default_route.side_effect = OutOfSyncError()

    with raises(OutOfSyncError):
        client.put(url, data={
            'ezone': 'overall',
            'intent': 'direct',
            'cluster_name': 'channel-stable-1',
        }, headers=headers)
    assert mocked_route_management.set_default_route.call_count == 3
