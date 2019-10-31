from __future__ import absolute_import

import json

from gevent import sleep
from kazoo.exceptions import NodeExistsError
from pytest import fixture, mark

from huskar_api.models import huskar_client
from huskar_api.models.container import ContainerManagement
from huskar_api.models.instance.schema import Instance
from huskar_api.models.exceptions import OutOfSyncError
from huskar_api.models.dataware.zookeeper import service_client
from ..utils import assert_response_ok


@fixture
def test_application_name(test_application, zk):
    zk.delete('/huskar/container', recursive=True)
    zk.delete('/huskar/container-barrier', recursive=True)
    try:
        yield test_application.application_name
    finally:
        zk.delete('/huskar/container', recursive=True)
        zk.delete('/huskar/container-barrier', recursive=True)


@fixture
def add_service(client, zk, test_application_name, test_application_token):
    def factory(key, value, runtime=None, token=None, cluster_name=None,
                extra_headers=None, version=None):
        data = {'key': key, 'value': value, 'runtime': runtime,
                'version': version}
        data = {k: v for k, v in data.iteritems() if v is not None}
        url = '/api/service/%s/%s' % (test_application_name,
                                      cluster_name or 'overall')
        headers = {'Authorization': token or test_application_token}
        if extra_headers:
            headers.update(extra_headers)
        response = client.post(url, data=data, headers=headers)
        return data, response

    return factory


def assert_response_value(item, value, runtime, whole):
    assert json.loads(item['value']) == json.loads(whole)
    if runtime is None:
        assert item['runtime'] is None
    else:
        assert json.loads(item['runtime']) == json.loads(runtime)


@mark.xparametrize('service_cluster_validate_failure')
def test_add_service_cluster_failure(
        add_service, cluster_name, error_response, key, value):

    _, r = add_service(key, value, cluster_name=cluster_name)
    assert r.status_code == error_response['status_code']
    assert json.loads(r.data)['message'] == error_response['message']


@mark.xparametrize('valid_service')
def test_add_service_instance(client, test_application_name, zk, add_service,
                              webhook_backends, add_webhook_subscriptions,
                              minimal_mode, key, value, runtime, whole,
                              last_audit_log):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    args, r = add_service(key, value, runtime)
    assert_response_ok(r)
    assert r.json['data']['meta']
    assert r.json['data']['value'] == json.loads(whole)

    children = zk.get_children(
        '/huskar/service/%s/overall' % test_application_name)
    assert children == [args['key']]

    instance_data, instance_stat = zk.get(
        '/huskar/service/%s/overall/%s' % (test_application_name, args['key']))
    assert json.loads(instance_data) == json.loads(whole)
    assert instance_stat.version == 0

    instance_children = zk.get_children(
        '/huskar/service/%s/overall/%s' % (test_application_name, args['key']))
    assert instance_children == []  # runtime node should not exist

    cm = ContainerManagement(huskar_client, key)
    assert cm.lookup() == []

    if not minimal_mode:
        audit_log = last_audit_log()
        assert audit_log.action_name == 'UPDATE_SERVICE'
        assert audit_log.action_json['application_name'] == \
            test_application_name
        assert audit_log.action_json['cluster_name'] == 'overall'
        assert audit_log.action_json['key'] == args['key']

        for result in webhook_backends:
            assert result['action_name'] == 'UPDATE_SERVICE'
            assert result['action_data']['application_name'] == \
                test_application_name
            assert result['action_data']['cluster_name'] == 'overall'
            assert result['action_data']['key'] == args['key']


def test_service_registry(
        client, zk, test_application_name, test_application_token,
        minimal_mode, last_audit_log):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    path = '/huskar/service/%s/stable/10.0.0.1_80' % test_application_name
    assert not zk.exists(path)

    url = '/api/data/service-registry'
    headers = {
        'Authorization': test_application_token, 'X-Cluster-Name': 'stable'}
    instance = {
        'ip': '10.0.0.1', 'port': {'main': 80}, 'state': 'up', 'meta': {}}
    data = {'key': '10.0.0.1_80', 'value': json.dumps(instance)}

    r = client.post(url, data=data, headers=headers)
    assert_response_ok(r)
    data, _ = zk.get(path)
    assert json.loads(data) == instance

    instance = dict(instance)
    instance['state'] = 'down'
    data = {'key': '10.0.0.1_80', 'runtime': json.dumps({'state': 'down'})}

    r = client.post(url, data=data, headers=headers)
    assert_response_ok(r)
    assert r.json['data']['meta']
    assert r.json['data']['value'] == instance
    data, _ = zk.get(path)
    assert json.loads(data) == instance

    if not minimal_mode:
        audit_log = last_audit_log()
        assert audit_log.action_name == 'UPDATE_SERVICE'
        print audit_log.action_json['data']['old']
        print audit_log.action_json['data']['new']
        assert json.loads(audit_log.action_json['data']['old']) == \
            dict(json.loads(audit_log.action_json['data']['new']),
                 **{'state': 'up'})


def test_service_registry_failed(
        client, zk, test_application_name, test_application_token, test_token):
    path = '/huskar/service/%s/stable/10.0.0.1_80' % test_application_name
    assert not zk.exists(path)

    url = '/api/data/service-registry'
    headers = {'Authorization': test_application_token}
    instance = {
        'ip': '10.0.0.1', 'port': {'main': 80}, 'state': 'up', 'meta': {}}
    data = {'key': '10.0.0.1_80', 'value': json.dumps(instance)}

    r = client.post(url, data=data, headers=headers)
    assert r.status_code == 403
    assert r.json['status'] == 'Forbidden'
    assert r.json['message'] == 'X-Cluster-Name is required'
    assert not zk.exists(path)

    headers = {'Authorization': test_token}
    r = client.post(url, data=data, headers=headers)
    assert r.status_code == 403
    assert r.json['status'] == 'Forbidden'
    assert r.json['message'] == \
        'Authorization with an application token is required'
    assert not zk.exists(path)


def test_service_registry_deregister(
        client, zk, test_application_name, test_application_token,
        minimal_mode):
    path = '/huskar/service/%s/stable/10.0.0.1_80' % test_application_name
    instance = {
        'ip': '10.0.0.1', 'port': {'main': 80}, 'state': 'up', 'meta': {}}
    zk.create(path, json.dumps(instance), makepath=True)

    url = '/api/data/service-registry'
    headers = {
        'Authorization': test_application_token, 'X-Cluster-Name': 'stable'}
    r = client.delete(url, data={'key': '10.0.0.1_80'}, headers=headers)
    assert_response_ok(r)
    assert not zk.exists(path)


@mark.xparametrize('valid_container_service')
def test_add_service_instance_from_container(
        client, test_application_name, zk, add_service, minimal_mode,
        key, value):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    args, r = add_service(key, value, cluster_name='stable')
    assert_response_ok(r)

    instance_data, instance_stat = zk.get(
        '/huskar/service/%s/stable/%s' % (test_application_name, key))
    assert json.loads(instance_data) == json.loads(value)
    assert instance_stat.version == 0

    cm = ContainerManagement(huskar_client, key)
    assert cm.lookup() == [(test_application_name, 'stable')]


@mark.xparametrize('valid_container_service')
def test_add_service_instance_from_container_but_meets_barrier(
        client, test_application_name, zk, add_service, minimal_mode,
        key, value):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    cm = ContainerManagement(huskar_client, key)
    cm.set_barrier()

    args, r = add_service(key, value, cluster_name='stable')
    assert r.status_code == 409
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == 'this container has been unbound recently'

    assert not zk.exists(
        '/huskar/service/%s/stable/%s' % (test_application_name, key))
    assert cm.lookup() == []


@mark.xparametrize('valid_service')
def test_add_service_with_cluster_duplicated_prefix(
        zk, mocker, add_service, test_application_name,
        key, value, runtime, whole):
    mocker.patch('huskar_api.settings.ROUTE_EZONE_LIST', ['alta1'])

    _, r = add_service(key, value, runtime, cluster_name='alta1-alta1-stable')
    assert r.status_code == 400
    assert r.json['status'] == 'DuplicatedEZonePrefixError'
    assert r.json['message'] == \
        'Cluster name should not contain duplicated E-Zone prefix.'
    assert r.json['data'] is None


@mark.xparametrize('invalid_service')
def test_add_service_validation(client, test_application_name, zk, add_service,
                                key, value, runtime, error_reason, status,
                                last_audit_log):
    args, r = add_service(key, value, runtime)

    assert r.status_code == 400, r.data
    assert r.json['status'] == status
    assert r.json['message'] == error_reason
    assert r.json['data'] is None

    node_exists = zk.exists(
        '/huskar/service/%s/overall/%s' % (test_application_name, args['key']))
    assert not node_exists, 'invalid node should not be added'

    assert last_audit_log() is None


@mark.xparametrize('valid_service')
def test_add_service_unauthorized(client, test_application_name, zk,
                                  add_service, stolen_application_token,
                                  test_user, test_token, minimal_mode,
                                  last_audit_log, key, value, runtime, whole):
    if minimal_mode:
        # In minimal mode, only non-app users will be restricted
        stolen_token = test_token
        stolen_username = test_user.username
    else:
        stolen_token = stolen_application_token
        stolen_username = 'stolen-{0}'.format(test_application_name)

    args, r = add_service(key, value, runtime, stolen_token)

    assert r.status_code == 400, r.data  # TODO should be 401
    assert r.json['status'] == 'NoAuthError'
    assert r.json['message'] == '{0} has no write authority on {1}'.format(
        stolen_username, test_application_name)
    assert r.json['data'] is None

    if not minimal_mode:
        assert last_audit_log() is None


@mark.xparametrize('valid_service')
def test_add_service_on_malformed_node(
        client, zk, test_application_name, test_application_token,
        add_service, key, value, runtime, whole):
    path = '/huskar/service/%s/stable/%s' % (test_application_name, key)
    zk.create(path, b'{"half-open', makepath=True)
    _, r = add_service(key, value, runtime, cluster_name='stable')
    assert_response_ok(r)
    data, stat = zk.get(path)
    assert json.loads(data)
    assert stat.version == 1


@mark.xparametrize('valid_service')
def test_delete_service(client, zk, test_application_name,
                        test_application_token, minimal_mode,
                        last_audit_log, key, value, runtime, whole):
    path = '/huskar/service/%s/beta/%s' % (test_application_name, key)
    zk.create(path, whole, makepath=True)
    assert zk.exists(path)

    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    url = '/api/service/%s/%s' % (test_application_name, 'beta')
    headers = {'Authorization': test_application_token}
    r = client.delete(url, data={'key': key}, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] is None

    assert not zk.exists(path)

    r = client.delete(url, data={'key': key + '1s'}, headers=headers)
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'service %s/beta/%s does not exist' % (
        test_application_name, key + '1s')

    if not minimal_mode:
        audit_log = last_audit_log()
        assert audit_log.action_name == 'DELETE_SERVICE'
        assert audit_log.action_json['application_name'] == \
            test_application_name
        assert audit_log.action_json['cluster_name'] == 'beta'
        assert audit_log.action_json['key'] == key


@mark.xparametrize('valid_container_service')
def test_delete_service_from_container(
        client, zk, test_application_name, test_application_token,
        minimal_mode, key, value):
    cm = ContainerManagement(huskar_client, key)

    path = '/huskar/service/%s/beta/%s' % (test_application_name, key)
    zk.create(path, json.dumps(value), makepath=True)
    cm.register_to(test_application_name, 'beta')

    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    assert zk.exists(path)
    assert cm.lookup() == [(test_application_name, 'beta')]

    url = '/api/service/%s/%s' % (test_application_name, 'beta')
    headers = {'Authorization': test_application_token}
    r = client.delete(url, data={'key': key}, headers=headers)
    assert_response_ok(r)

    assert not zk.exists(path)
    assert cm.lookup() == []


@mark.xparametrize('valid_service')
def test_delete_service_which_does_not_exist(client, zk, test_application_name,
                                             test_application_token,
                                             minimal_mode, last_audit_log,
                                             key, value, runtime, whole):
    path = '/huskar/service/%s/beta/%s' % (test_application_name, key)
    assert not zk.exists(path)

    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    url = '/api/service/%s/%s' % (test_application_name, 'beta')
    headers = {'Authorization': test_application_token}
    r = client.delete(url, data={'key': key}, headers=headers)
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'].endswith('does not exist')
    assert r.json['data'] is None

    assert not zk.exists(path)

    if not minimal_mode:
        assert last_audit_log() is None


@mark.xparametrize('valid_service')
def test_get_service_instance(client, zk, test_application_name,
                              test_application_token, minimal_mode,
                              key, value, runtime, whole):
    path = '/huskar/service/%s/beta/%s' % (test_application_name, key)
    zk.create(path, whole, makepath=True)
    assert zk.exists(path)

    url = '/api/service/%s/%s' % (test_application_name, 'beta')
    headers = {'Authorization': test_application_token}
    r = client.get(url, query_string={'key': key}, headers=headers)
    assert_response_ok(r)
    assert r.json['data']['application'] == test_application_name
    assert r.json['data']['cluster'] == 'beta'
    assert r.json['data']['key'] == key
    assert_response_value(r.json['data'], value, runtime, whole)


def test_get_service_instance_tolerance(client, zk, test_application_name,
                                        test_application_token, minimal_mode):
    path = '/huskar/service/%s/beta/wtf' % test_application_name
    zk.create(path, '100', makepath=True)
    assert zk.exists(path)

    url = '/api/service/%s/%s' % (test_application_name, 'beta')
    headers = {'Authorization': test_application_token}
    r = client.get(url, query_string={'key': 'wtf'}, headers=headers)
    assert_response_ok(r)
    assert r.json['data']['application'] == test_application_name
    assert r.json['data']['cluster'] == 'beta'
    assert r.json['data']['key'] == 'wtf'
    assert r.json['data']['value'] == '100'
    assert r.json['data']['runtime'] is None


@mark.xparametrize('valid_service')
def test_get_service_unauthorized(client, test_application_name, zk,
                                  add_service, stolen_application_token,
                                  minimal_mode,
                                  key, value, runtime, whole):
    # The service information is public
    test_get_service_instance(
        client, zk, test_application_name, stolen_application_token,
        minimal_mode, key, value, runtime, whole)


@mark.xparametrize('valid_service')
def test_get_service_instance_from_linked_cluster(
        client, test_application_name, test_application_token, zk, add_service,
        minimal_mode, key, value, runtime, whole):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    # "route" should be ignored because we don't pass "X-SOA-Mode:route"
    path = '/huskar/service/%s/stable' % test_application_name
    data = '{"link":["alta-foo"],"route":{"%s":"no"}}' % test_application_name
    zk.create(path, data, makepath=True)

    path = '/huskar/service/%s/alta-foo/%s' % (test_application_name, key)
    zk.create(path, whole, makepath=True)
    assert zk.exists(path)

    url = '/api/service/%s/stable' % test_application_name
    headers = {'Authorization': test_application_token}
    r = client.get(url, query_string={'key': key}, headers=headers)
    assert_response_ok(r)
    assert r.json['data']['application'] == test_application_name
    assert r.json['data']['cluster'] == 'stable'
    assert r.json['data']['cluster_physical_name'] == 'alta-foo'
    assert r.json['data']['key'] == key
    assert_response_value(r.json['data'], value, runtime, whole)


@mark.xparametrize('valid_service')
def test_get_service_instance_from_linked_cluster_not_resolve(
        client, test_application_name, test_application_token, zk, add_service,
        minimal_mode, key, value, runtime, whole):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    # "route" and "link" should be ignored because we
    # don't pass "X-SOA-Mode:route" and passed "resolve=0"
    path = '/huskar/service/%s/stable' % test_application_name
    data = '{"link":["alta-foo"],"route":{"%s":"no"}}' % test_application_name
    zk.create(path, data, makepath=True)

    path = '/huskar/service/%s/stable/%s' % (test_application_name, key)
    zk.create(path, whole, makepath=True)
    assert zk.exists(path)

    url = '/api/service/%s/stable' % test_application_name
    headers = {'Authorization': test_application_token}
    r = client.get(url, query_string={'key': key, 'resolve': '0'},
                   headers=headers)
    assert_response_ok(r)
    assert r.json['data']['application'] == test_application_name
    assert r.json['data']['cluster'] == 'stable'
    assert 'cluster_physical_name' not in r.json['data']
    assert r.json['data']['key'] == key
    assert_response_value(r.json['data'], value, runtime, whole)


@mark.xparametrize('valid_service')
def test_get_service_instance_with_route_mode(
        client, test_application_name, test_application_token, zk, add_service,
        minimal_mode, key, value, runtime, whole):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    path = '/huskar/service/%s/stable' % test_application_name
    data = '{"route":{"%s": "alta-foo"}}' % test_application_name
    zk.create(path, data, makepath=True)

    path = '/huskar/service/%s/alta-foo/%s' % (test_application_name, key)
    zk.create(path, whole, makepath=True)
    assert zk.exists(path)

    url = '/api/service/%s/direct' % test_application_name

    headers = {'Authorization': test_application_token}
    r = client.get(url, query_string={'key': key}, headers=headers)
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'

    headers = {'Authorization': test_application_token, 'X-SOA-Mode': 'foo'}
    r = client.get(url, query_string={'key': key}, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'X-SOA-Mode must be one of orig/prefix/route'

    headers = {'Authorization': test_application_token, 'X-SOA-Mode': 'route'}
    r = client.get(url, query_string={'key': key}, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == (
        'X-Cluster-Name is required while X-SOA-Mode is route')

    headers = {'Authorization': test_application_token, 'X-SOA-Mode': 'route',
               'X-Cluster-Name': 'stable'}
    r = client.get(url, query_string={'key': key}, headers=headers)
    assert_response_ok(r)
    assert r.json['data']['application'] == test_application_name
    assert r.json['data']['cluster'] == 'direct'
    assert r.json['data']['cluster_physical_name'] == 'alta-foo'
    assert r.json['data']['key'] == key
    assert_response_value(r.json['data'], value, runtime, whole)


@mark.xparametrize('valid_service')
@mark.parametrize('cluster_info', [
    '{"link":["alta-foo"]}',
    '{"route":{"%(name)s":"alta-foo"}}',
])
def test_get_service_list_from_linked_cluster(
        client, test_application_name, test_application_token, zk, add_service,
        minimal_mode, cluster_info, key, value, runtime, whole):
    zk.set('/huskar/service/%s' % test_application_name,
           value='{"default_route":{"overall":{"direct":"stable"}}}')
    if minimal_mode:
        sleep(0.1)

    path = '/huskar/service/%s/stable' % test_application_name
    data = cluster_info % {'name': test_application_name}
    zk.create(path, data, makepath=True)

    # create an instance inside symlink cluster
    path = ('/huskar/service/%s/stable/should_not_appear' %
            test_application_name)
    zk.create(
        path, '{"ip":"169.254.255.255","port":{"main":80}}', makepath=True)

    # create an instance inside physical cluster
    path = '/huskar/service/%s/alta-foo/%s' % (test_application_name, key)
    zk.create(path, whole, makepath=True)
    assert zk.exists(path)

    headers = {'Authorization': test_application_token, 'X-SOA-Mode': 'route',
               'X-Cluster-Name': 'stable'}
    r = client.get(
        '/api/service/%s/direct' % test_application_name, headers=headers)
    assert_response_ok(r)
    assert key in {d['key'] for d in r.json['data']}
    assert 'should_not_appear' not in {d['key'] for d in r.json['data']}

    assert len(r.json['data']) == 1
    assert_response_value(r.json['data'][0], value, runtime, whole)


@mark.xparametrize('valid_service')
@mark.parametrize('cluster_info', [
    '{"link":["alta-foo"]}',
    '{"route":{"%(name)s":"stable"}}',
])
def test_get_service_list_from_linked_cluster_not_resolve(
        client, test_application_name, test_application_token, zk, add_service,
        minimal_mode, cluster_info, key, value, runtime, whole):
    zk.set('/huskar/service/%s' % test_application_name,
           value='{"default_route":{"overall":{"direct":"stable"}}}')
    if minimal_mode:
        sleep(0.1)

    path = '/huskar/service/%s/stable' % test_application_name
    data = cluster_info % {'name': test_application_name}
    zk.create(path, data, makepath=True)

    path = '/huskar/service/%s/stable/%s' % (test_application_name, key)
    zk.create(path, whole, makepath=True)
    assert zk.exists(path)

    headers = {'Authorization': test_application_token, 'X-SOA-Mode': 'route',
               'X-Cluster-Name': 'stable'}
    r = client.get(
        '/api/service/%s/direct?resolve=0' % test_application_name,
        headers=headers)
    assert_response_ok(r)
    assert len(r.json['data']) == 0

    r = client.get(
        '/api/service/%s/stable?resolve=0' % test_application_name,
        headers=headers)
    assert_response_ok(r)
    assert key in {d['key'] for d in r.json['data']}
    assert 'should_not_appear' not in {d['key'] for d in r.json['data']}

    assert len(r.json['data']) == 1
    assert_response_value(r.json['data'][0], value, runtime, whole)


@mark.xparametrize('valid_service')
def test_add_service_to_linked_cluster(
        client, test_application_name, add_service, zk, minimal_mode,
        key, value, runtime, whole):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    path = '/huskar/service/%s/overall' % test_application_name
    zk.create(path, '{"link":["alta-foo"]}', makepath=True)

    path = '/huskar/service/%s/alta-foo' % test_application_name
    zk.create(path, makepath=True)

    args, r = add_service(key, value, runtime)

    assert_response_ok(r)
    assert r.json['data']['meta']
    assert r.json['data']['value'] == json.loads(whole)

    instances = zk.get_children(
        '/huskar/service/%s/alta-foo' % test_application_name)
    assert instances == [], 'should never register to a physical cluster'

    instances = zk.get_children(
        '/huskar/service/%s/overall' % test_application_name)
    assert instances == [key]

    instance_data, instance_stat = zk.get(
        '/huskar/service/%s/overall/%s' % (test_application_name, args['key']))
    instance_data = json.loads(instance_data)
    expected_data = json.loads(args['value'])
    meta = expected_data.setdefault('meta', {})
    expected_data['meta'] = {
        key: str(val) if val is not None else ''
        for key, val in meta.items()
    }
    instance_data.pop('state', None)  # We don't care which state is here
    expected_data = {key: expected_data[key] for key in instance_data}
    assert instance_data == expected_data
    assert instance_stat.version == 0


@mark.xparametrize('valid_service')
def test_delete_service_from_linked_cluster(
        client, test_application_name, test_application_token, add_service,
        zk, minimal_mode, key, value, runtime, whole):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    path = '/huskar/service/%s/overall' % test_application_name
    zk.create(path, '{"link":["alta-foo"]}', makepath=True)

    path = '/huskar/service/%s/overall/%s' % (test_application_name, key)
    zk.create(path, value, makepath=True)

    path = '/huskar/service/%s/alta-foo/%s' % (test_application_name, key)
    zk.create(path, value, makepath=True)

    url = '/api/service/%s/%s' % (test_application_name, 'overall')
    headers = {'Authorization': test_application_token}
    r = client.delete(url, data={'key': key}, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] is None

    # We should never delete from a physical cluster
    assert zk.get_children(
        '/huskar/service/%s/overall' % test_application_name) == []
    assert zk.get_children(
        '/huskar/service/%s/alta-foo' % test_application_name) == [key]

    instance_data, instance_stat = zk.get(
        '/huskar/service/%s/alta-foo/%s' % (test_application_name, key))
    assert instance_data == value
    assert instance_stat.version == 0


@mark.xparametrize('valid_service')
def test_get_service_list(client, zk, test_application_name,
                          test_application_token, minimal_mode,
                          key, value, runtime, whole):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    path = '/huskar/service/%s/beta/%s' % (test_application_name, key)
    zk.create(path, whole, makepath=True)
    assert zk.exists(path)

    another_path = '/huskar/service/%s/beta/such' % test_application_name
    zk.create(another_path, 'doge', makepath=True)
    _, stat = zk.get(another_path)

    url = '/api/service/%s/%s' % (test_application_name, 'beta')
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert len(r.json['data']) == 2

    data_map = {d['key']: d for d in r.json['data']}
    assert set(data_map) == {'such', key}
    assert data_map['such'] == {
        'application': test_application_name,
        'cluster': 'beta',
        'key': 'such',
        'value': 'doge',
        'runtime': None,
        'meta': {
            'created': int(stat.created * 1000),
            'last_modified': int(stat.last_modified * 1000),
            'version': 0,
        },
    }
    assert data_map[key]['application'] == test_application_name
    assert data_map[key]['cluster'] == 'beta'
    assert data_map[key]['key'] == key
    assert_response_value(data_map[key], value, runtime, whole)


@mark.xparametrize('valid_service')
def test_get_cluster_list(client, zk, test_application_name,
                          test_application_token, minimal_mode,
                          key, value, runtime, whole):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    for cluster in ('alpha', 'beta'):
        path = '/huskar/service/%s/%s/%s' % (
            test_application_name, cluster, key)
        zk.create(path, whole, makepath=True)
        assert zk.exists(path)

    path = '/huskar/service/%s/broken' % test_application_name
    zk.create(path, 'xxx', makepath=True)
    path = '/huskar/service/%s/latest' % test_application_name
    zk.create(path, '{"link":["alpha"],"route":{"a":"b"}}', makepath=True)
    del path

    url = '/api/service/%s' % test_application_name
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert len(r.json['data']) == 4

    data_map = {d['name']: (
        d['physical_name'],
        d['route'],
        d['meta'].get('instance_count'),
        d['meta'].get('is_symbol_only'),
    ) for d in r.json['data']}
    assert data_map == {
        'alpha': (None, [], 1, None),
        'beta': (None, [], 1, None),
        'latest': ('alpha', [
            {'application_name': 'a', 'intent': 'direct', 'cluster_name': 'b'},
        ], None, True),
        'broken': (None, [], 0, None),
    }


@mark.xparametrize('valid_service')
def test_batch_get_service_list(client, zk, test_application_name,
                                test_application_token, minimal_mode,
                                key, value, runtime, whole):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    for cluster_name in 'alpha', 'beta':
        path = '/huskar/service/%s/%s/%s' % (
            test_application_name, cluster_name, key)
        zk.create(path, whole, makepath=True)

    url = '/api/batch_service?application=%s' % test_application_name
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert len(r.json['data']) == 2

    data_map = {(d['key'], d['cluster']): d for d in r.json['data']}
    assert set(data_map) == {(key, 'alpha'), (key, 'beta')}
    assert_response_value(data_map[key, 'alpha'], value, runtime, whole)
    assert_response_value(data_map[key, 'beta'], value, runtime, whole)


@mark.xparametrize('valid_runtime_patch')
def test_patch_runtime(client, test_application_name, zk, add_service,
                       minimal_mode, key, present, patch, result):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    path = '/huskar/service/%s/overall/%s' % (test_application_name, key)
    zk.create(path, present['value'], makepath=True)

    _, r = add_service(key, None, patch)  # pass runtime without value
    assert_response_ok(r)
    assert r.json['data']['meta']
    assert r.json['data']['value'] == result['value']
    assert json.loads(zk.get(path)[0]) == result['value']


@mark.xparametrize('invalid_runtime_patch')
def test_patch_runtime_validation(client, test_application_name, zk,
                                  add_service, minimal_mode,
                                  key, patch, error_reason, status):
    if minimal_mode:
        zk.ensure_path('/huskar/service/%s' % test_application_name)
        sleep(0.1)

    _, r = add_service(key, None, patch)
    assert r.status_code == 400, r.data
    assert r.json['status'] == status
    assert r.json['message'] == error_reason
    assert r.json['data'] is None


def test_add_service_instance_with_version(
        test_application_name, add_service, zk):
    key = '169.254.1.2_5000'
    value = '{"ip": "169.254.1.2", "port":{"main": 5000},"state":"up"}'
    path = '/huskar/service/%s/overall/%s' % (test_application_name, key)
    zk.create(path, makepath=True)
    _, stat = zk.get(path)

    version = stat.version
    args, r = add_service(key, value, version=version)
    assert_response_ok(r)
    args, r = add_service(key, value, version=version)
    assert r.status_code == 409
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == 'resource is modified by another request'


def test_add_service_instance_with_concurrency_request_error(
        test_application_name, add_service, mocker):
    key = '169.254.1.2_5000'
    value = '{"ip": "169.254.1.2", "port":{"main": 5000},"state":"up"}'
    mocker.patch.object(
        service_client.raw_client, 'create', side_effect=NodeExistsError)

    _, r = add_service(key, value)
    assert r.status_code == 409
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == 'resource is modified by another request'

    mocker.patch.object(service_client.raw_client, 'create', return_value=None)
    mocker.patch.object(service_client.raw_client, 'exists', return_value=None)

    _, r = add_service(key, value)
    assert r.status_code == 409
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == 'resource is modified by another request'


def test_get_weight(client, zk, test_application_name, test_application_token):
    key = '169.254.1.2_5000'
    value = '{"ip": "169.254.1.2", "port":{"main": 5000},"state":"up"}'
    args = (test_application_name, key)
    zkpath = '/huskar/service/%s/alpha_stable/%s' % args
    url = '/api/service/%s/alpha_stable/%s/weight' % args
    headers = {'Authorization': test_application_token}

    zk.create(zkpath, value, makepath=True)
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] == {'weight': 0}

    value = (
        '{"ip": "169.254.1.2", "port":{"main": 5000},"state":"up",'
        '"meta":{"weight": "10"}}')
    zk.set(zkpath, value)
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] == {'weight': 10}

    zk.set(zkpath, 'broken')
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] == {'weight': 0}

    url = '/api/service/%s/alpha_dev/%s/weight' % args
    r = client.get(url, headers=headers)
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'service %s/alpha_dev/%s does not exist' % (
        test_application_name, key)


def test_set_weight(
        client, zk, mocker, test_application_name, test_application_token):
    key = '169.254.1.2_5000'
    value = '{"ip": "169.254.1.2", "port":{"main": 5000},"state":"up"}'
    args = (test_application_name, key)
    zkpath = '/huskar/service/%s/alpha_stable/%s' % args
    url = '/api/service/%s/alpha_stable/%s/weight' % args
    headers = {'Authorization': test_application_token}

    # create
    zk.create(zkpath, value, makepath=True)
    r = client.post(url, headers=headers, data={
        'weight': '10',
        'ephemeral': '1',
    })
    assert_response_ok(r)

    data, stat = zk.get(zkpath)
    assert json.loads(data)['meta']['weight'] == '10'
    assert stat.version == 1

    # update
    r = client.post(url, headers=headers, data={
        'weight': '200',
        'ephemeral': '1',
    })
    assert_response_ok(r)

    data, stat = zk.get(zkpath)
    assert json.loads(data)['meta']['weight'] == '200'
    assert stat.version == 2

    r = client.post(url, headers=headers, data={
        'weight': '-1',
        'ephemeral': '1',
    })
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'weight must be a positive integer'

    r = client.post(url, headers=headers, data={
        'ephemeral': '1',
    })
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'weight must be a positive integer'

    r = client.post(url, headers=headers, data={
        'weight': '1',
    })
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'ephemeral must be "1" for now'

    zk.set(zkpath, 'broken')
    r = client.post(url, headers=headers, data={
        'weight': '1',
        'ephemeral': '1',
    })
    assert r.status_code == 500
    assert r.json['status'] == 'InternalServerError'

    zk.set(zkpath, value)
    with mocker.patch.object(Instance, 'save', side_effect=OutOfSyncError()):
        r = client.post(url, headers=headers, data={
            'weight': '1',
            'ephemeral': '1',
        })
    assert r.status_code == 409
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == (
        'service %s/alpha_stable/%s has been modified by another request' % (
            test_application_name, key)
    )

    url = '/api/service/%s/alpha_dev/%s/weight' % args
    r = client.post(url, headers=headers, data={
        'weight': '1',
        'ephemeral': '1',
    })
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'service %s/alpha_dev/%s does not exist' % (
        test_application_name, key)
