from __future__ import absolute_import

import io
import json
import time

from gevent import sleep
from pytest import fixture, mark
from sqlalchemy.exc import OperationalError

from huskar_api import settings
from huskar_api.models.auth import Application
from huskar_api.service import comment as comment_svc
from huskar_api.switch import switch, SWITCH_ENABLE_CONFIG_PREFIX_BLACKLIST
from ..utils import assert_response_ok


def pytest_generate_tests(metafunc):
    metafunc.parametrize('data_type', ['config', 'switch'])


@fixture(autouse=True)
def mocked_ezone_prefix(mocker):
    return mocker.patch('huskar_api.settings.ROUTE_EZONE_LIST', ['alta1'])


@fixture
def test_application_name(test_application):
    return test_application.application_name


@fixture
def build_zk_tree(zk, test_application_name, data_type):
    def factory(nodes):
        for path, value in nodes:
            path = '/huskar/%s/%s%s' % (data_type, test_application_name, path)
            value = value.encode('utf-8')
            zk.create(path.strip('/'), value, makepath=True)
    return factory


@fixture
def build_comments(db, test_application_name, data_type):
    def factory(nodes):
        for path, comment in nodes:
            if not comment:
                continue
            cluster, key = path.strip('/').split('/')
            comment_svc.save(
                data_type, test_application_name, cluster, key, comment)
    return factory


@fixture
def inspect_comment(test_application_name, data_type):
    def factory(path):
        cluster, key = path.strip('/').split('/')
        return comment_svc.get(
            data_type, test_application_name, cluster, key)
    return factory


@mark.xparametrize
def test_add_data(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, build_comments, inspect_comment, webhook_backends,
        add_webhook_subscriptions, minimal_mode, last_audit_log, old_tree,
        new_tree, adding_args, data_type):
    build_zk_tree((node['path'], node['value']) for node in old_tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)
    else:
        build_comments(
            (node['path'], node.get('comment')) for node in old_tree)

    for args in adding_args:
        cluster = args['cluster']
        data = {'key': args['key'], 'value': args['value']}
        if 'comment' in args:
            data['comment'] = args['comment']
        for method in 'POST', 'PUT':
            url = '/api/%s/%s/%s' % (data_type, test_application_name, cluster)
            headers = {'Authorization': test_application_token}
            r = client.open(url, method=method, data=data, headers=headers)
            assert_response_ok(r)
            assert r.json['data'] is None

    for node in new_tree:
        path = '/huskar/%s/%s%s' % (
            data_type, test_application_name, node['path'])
        if 'children' in node:
            children = zk.get_children(path.strip('/'))
            assert set(children) == set(node['children'])
        if 'value' in node:
            data, _ = zk.get(path.strip('/'))
            assert data == node['value'].encode('utf-8')
        if 'comment' in node and not minimal_mode:
            assert inspect_comment(node['path']) == node['comment']

    if not minimal_mode:
        audit_log = last_audit_log()
        assert audit_log.action_name == 'UPDATE_%s' % data_type.upper()
        assert audit_log.action_json['application_name'] == \
            test_application_name
        assert audit_log.action_json['cluster_name'] == cluster
        assert len(webhook_backends) == len(adding_args) * 4
        for result in webhook_backends:
            assert (result['action_name'] ==
                    'UPDATE_%s' % data_type.upper())


def test_add_data_with_cluster_duplicated_prefix(
        client, test_application_name, test_application_token, zk, data_type):
    data = {'key': 'test_foo', 'value': '0.1'}
    url = '/api/%s/%s/alta1-alta1-stable' % (data_type, test_application_name)
    headers = {'Authorization': test_application_token}
    r = client.post(url, data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'DuplicatedEZonePrefixError'
    assert r.json['message'] == \
        'Cluster name should not contain duplicated E-Zone prefix.'
    assert r.json['data'] is None


@mark.parametrize('exist', [False, True])
@mark.parametrize('key,prefix', [
    ('FX_foo', 'FX_'),
    ('HUSKAR233', 'HUSKAR'),
])
@mark.parametrize('blacklisted', [True, False])
def test_add_data_with_config_key_prefix_blacklisted(
        client, test_application_name, test_application_token, exist,
        key, prefix, zk, data_type, mocker, blacklisted):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_CONFIG_PREFIX_BLACKLIST:
            return True
        return default
    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    if not blacklisted:
        mocker.patch.object(settings, 'CONFIG_PREFIX_BLACKLIST', [])
    else:
        mocker.patch.object(
            settings, 'CONFIG_PREFIX_BLACKLIST', ['HUSKAR', 'FX_'])
    key = '{}.{}'.format(key, time.time())
    if exist:
        zk.ensure_path(
            '/huskar/{}/{}/alta1-stable/{}'.format(
                data_type, test_application_name, key))

    data = {'key': key, 'value': '0.1'}
    url = '/api/%s/%s/alta1-stable' % (data_type, test_application_name)
    headers = {'Authorization': test_application_token}
    r = client.post(url, data=data, headers=headers)

    if exist or data_type != 'config' or not blacklisted:
        assert_response_ok(r)
    else:
        assert r.status_code == 400
        assert r.json['status'] == 'BadRequest'
        assert r.json['message'] == (
            'The key {key} starts with {prefix} is denied.'.format(
                key=key, prefix=prefix))
        assert r.json['data'] is None


def test_add_data_with_version(
        client, test_application_name, test_application_token, zk, data_type,
        build_zk_tree):
    cluster_name, key, value = 'overall', 'foo', b'bar'
    path = '/huskar/%s/%s/%s/%s' % (
        data_type, test_application_name, cluster_name, key)
    zk.create(path, makepath=True)
    url = '/api/%s/%s/%s' % (data_type, test_application_name, cluster_name)
    headers = {'Authorization': test_application_token}
    _, stat = zk.get(path)
    data = {
        'key': key,
        'value': value,
        'version': stat.version,
    }
    r = client.post(url, data=data, headers=headers)
    assert_response_ok(r)


def test_add_data_with_oudated_version(
        client, test_application_name, test_application_token, zk, data_type,
        build_zk_tree):
    cluster_name, key, value = 'overall', 'foo', b'bar'
    path = '/huskar/%s/%s/%s/%s' % (
        data_type, test_application_name, cluster_name, key)
    zk.create(path, makepath=True)

    url = '/api/%s/%s/%s' % (data_type, test_application_name, cluster_name)
    headers = {'Authorization': test_application_token}
    _, stat = zk.get(path)
    data = {
        'key': key,
        'value': value,
        'version': stat.version - 2,
    }
    r = client.post(url, data=data, headers=headers)
    assert r.status_code == 409
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == 'resource is modified by another request'


@mark.xparametrize
def test_delete_data(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, build_comments, inspect_comment, webhook_backends,
        add_webhook_subscriptions, minimal_mode, last_audit_log, old_tree,
        new_tree, deleting_args, data_type):
    build_zk_tree((node['path'], node['value']) for node in old_tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)
    else:
        build_comments(
            (node['path'], node.get('comment')) for node in old_tree)

    for args in deleting_args:
        cluster = args['cluster']
        url = '/api/%s/%s/%s' % (data_type, test_application_name, cluster)
        data = {'key': args['key']}
        headers = {'Authorization': test_application_token}
        r = client.delete(url, data=data, headers=headers)
        assert_response_ok(r)
        assert r.json['data'] is None

    for node in new_tree:
        path = '/huskar/%s/%s%s' % (
            data_type, test_application_name, node['path'])
        if 'children' in node:
            assert zk.get_children(path.strip('/')) == node['children']
        if 'value' in node:
            data, _ = zk.get(path.strip('/'))
            assert data == node['value']
        if 'comment' in node and not minimal_mode:
            assert inspect_comment(node['path']) == node['comment']

    if not minimal_mode:
        new_paths = {node['path'] for node in new_tree}
        gone_comments = [
            node['path'] for node in old_tree
            if node['path'] not in new_paths
            if 'comment' in node
        ]
        for path in gone_comments:
            assert inspect_comment(path) == ''

        audit_log = last_audit_log()
        assert audit_log.action_name == 'DELETE_%s' % data_type.upper()
        assert audit_log.action_json['application_name'] == \
            test_application_name
        assert audit_log.action_json['cluster_name'] == cluster

        assert len(webhook_backends) == len(deleting_args) * 2
        for result in webhook_backends:
            assert (result['action_name'] ==
                    'DELETE_%s' % data_type.upper())


def test_delete_data_which_does_not_exist(
        client, zk, test_application_name, test_application_token, data_type):
    url = '/api/%s/%s/overall' % (data_type, test_application_name)
    data = {'key': 'meow'}
    headers = {'Authorization': test_application_token}
    r = client.delete(url, data=data, headers=headers)
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == '%s %s/overall/meow does not exist' % (
        data_type, test_application_name)
    assert r.json['data'] is None


@mark.xparametrize
def test_get_data(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, minimal_mode, tree, result, data_type):
    build_zk_tree((node['path'], node['value']) for node in tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)

    for item in result:
        url = '/api/%s/%s/%s?key=%s' % (
            data_type, test_application_name, item['cluster'], item['key'])
        headers = {'Authorization': test_application_token}
        r = client.get(url, headers=headers)
        assert_response_ok(r)

        internal_cluster = item.get('internal_cluster', item['cluster'])
        _, stat = zk.get('/huskar/%s/%s/%s/%s' % (
            data_type, test_application_name, internal_cluster, item['key']))
        expected_data = {
            'application': test_application_name,
            'cluster': item['cluster'],
            'key': item['key'],
            'value': item['value'],
            'meta': {
                'created': int(stat.created * 1000),
                'last_modified': int(stat.last_modified * 1000),
                'version': int(stat.version),
            },
            'comment': ''}
        if minimal_mode:
            expected_data.pop('comment')
        assert r.json['data'] == expected_data


@mark.xparametrize
def test_get_data_failed(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, minimal_mode, tree, result, data_type):
    build_zk_tree((node['path'], node['value']) for node in tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)

    for item in result:
        url = '/api/%s/%s/%s?key=%s' % (
            data_type, test_application_name, item['cluster'], item['key'])
        headers = {'Authorization': test_application_token}
        r = client.get(url, headers=headers)
        assert r.status_code == item['status_code']
        assert r.json['status'] == item['status_text']
        assert r.json['message'] == item['message'] % {
            'data_type': data_type,
            'test_application_name': test_application_name,
        }
        assert r.json['data'] is None


def test_get_data_from_public_domain_application(
        client, zk, faker, test_application, test_application_token,
        data_type):
    name = 'public.%s' % faker.uuid4()
    application = Application.create(name, test_application.team_id)
    path = '/huskar/%s/%s/foo/bar' % (data_type, application.application_name)
    url = '/api/%s/%s/foo?key=bar' % (data_type, application.application_name)
    zk.create(path, b'1', makepath=True)

    r = client.get(url, headers={'Authorization': test_application_token})
    assert_response_ok(r)


@mark.xparametrize
def test_get_multi(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, minimal_mode, tree, result, data_type):
    build_zk_tree((node['path'], node['value']) for node in tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)

    for item in result:
        def make_meta(pair):
            _, stat = zk.get('/huskar/%s/%s/%s/%s' % (
                data_type, test_application_name, item['cluster'],
                pair['key']))
            return {
                'created': int(stat.created * 1000),
                'last_modified': int(stat.last_modified * 1000),
                'version': int(stat.version)}

        url = '/api/%s/%s/%s' % (
            data_type, test_application_name, item['cluster'])
        headers = {'Authorization': test_application_token}
        r = client.get(url, headers=headers)
        assert_response_ok(r)
        assert sorted(r.json['data']) == sorted([
            {
                'application': test_application_name,
                'cluster': item['cluster'],
                'key': pair['key'],
                'value': pair['value'],
                'meta': make_meta(pair),
            }
            if minimal_mode else
            {
                'application': test_application_name,
                'cluster': item['cluster'],
                'key': pair['key'],
                'value': pair['value'],
                'meta': make_meta(pair),
                'comment': '',
            }
            for pair in item['pairs']
        ])


@mark.xparametrize
def test_get_clusters(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, minimal_mode, tree, clusters, data_type):
    build_zk_tree((node['path'], node['value']) for node in tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)

    url = '/api/%s/%s' % (data_type, test_application_name)
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] == [{'name': cluster} for cluster in clusters]


def test_get_clusters_failed(client, test_application_token, data_type):
    url = '/api/%s/test ' % data_type
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'ValidationError'
    assert r.json['message'] == (
        '{"application": ["AppID(test ) should consist by most 128 '
        'characters of numbers, lowercase letters and underscore."]}')


@mark.xparametrize
def test_create_clusters(
        client, zk, test_application_name, test_application_token,
        last_audit_log, build_zk_tree, webhook_backends,
        add_webhook_subscriptions, minimal_mode, tree, name, expected,
        data_type):
    build_zk_tree((node['path'], node['value']) for node in tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)

    r = client.post(
        '/api/%s/%s' % (data_type, test_application_name),
        data={'cluster': name},
        headers={'Authorization': test_application_token})
    assert r.status_code == expected['code']
    assert r.json['status'] == expected['status']
    assert r.json['message'] == expected['message']
    assert r.json['data'] is None

    if not minimal_mode:
        audit_log = last_audit_log()
        if r.json['status'] == 'SUCCESS':
            assert audit_log.action_name == \
                'CREATE_%s_CLUSTER' % data_type.upper()
            assert audit_log.action_json['application_name'] == \
                test_application_name
            assert audit_log.action_json['cluster_name'] == name
            sleep(0.1)
            assert len(webhook_backends) == 2
            for result in webhook_backends:
                assert (result['action_name'] ==
                        'CREATE_%s_CLUSTER' % data_type.upper())
        else:
            assert audit_log is None


def test_create_clusters_with_duplicated_prefix(
        client, test_application_name, test_application_token, data_type):
    url = '/api/%s/%s' % (data_type, test_application_name)
    data = {'cluster': 'alta1-alta1-stable'}
    headers = {'Authorization': test_application_token}
    r = client.post(url, data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'DuplicatedEZonePrefixError'
    assert r.json['message'] == \
        'Cluster name should not contain duplicated E-Zone prefix.'
    assert r.json['data'] is None


@mark.xparametrize
def test_delete_clusters(
        client, zk, test_application_name, test_application_token,
        last_audit_log, build_zk_tree, webhook_backends,
        add_webhook_subscriptions, minimal_mode, tree, name, expected,
        data_type):
    build_zk_tree((node['path'], node['value']) for node in tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)

    r = client.delete(
        '/api/%s/%s' % (data_type, test_application_name),
        data={'cluster': name},
        headers={'Authorization': test_application_token})
    assert r.status_code == expected['code']
    assert r.json['status'] == expected['status']
    assert r.json['message'] == expected['message']
    assert r.json['data'] is None

    if not minimal_mode:
        audit_log = last_audit_log()
        if r.json['status'] == 'SUCCESS':
            assert audit_log.action_name == \
                'DELETE_%s_CLUSTER' % data_type.upper()
            assert audit_log.action_json['application_name'] == \
                test_application_name
            sleep(0.1)
            assert len(webhook_backends) == 2
            for result in webhook_backends:
                assert (result['action_name'] ==
                        'DELETE_%s_CLUSTER' % data_type.upper())
            assert audit_log.action_json['cluster_name'] == name
        else:
            assert audit_log is None


@mark.xparametrize
def test_batch_export(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, build_comments, minimal_mode, tree, result, data_type):
    build_zk_tree((node['path'], node['value']) for node in tree)
    if minimal_mode:
        zk.ensure_path('/huskar/%s/%s' % (data_type, test_application_name))
        sleep(0.1)
    else:
        build_comments((node['path'], node.get('comment')) for node in tree)

    result = [dict(r) for r in result]
    for r in result:
        r.setdefault('application', test_application_name)
    if minimal_mode:
        for r in result:
            r.pop('comment', None)

    url = '/api/batch_%s?application=%s' % (data_type, test_application_name)
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert all(item['meta']['version'] > -1 for item in r.json['data'])
    assert sorted(
        {k: v for k, v in item.iteritems() if k != 'meta'}
        for item in r.json['data']
    ) == sorted(result)

    url = '/api/batch_%s?application=%s&cluster=beta' % (
        data_type, test_application_name)
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert all(item['meta']['version'] > -1 for item in r.json['data'])
    assert sorted(
        {k: v for k, v in item.iteritems() if k != 'meta'}
        for item in r.json['data']
    ) == sorted(r for r in result if r['cluster'] == 'beta')

    url = '/api/batch_%s?application=%s&format=file' % (
        data_type, test_application_name)
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert r.status_code == 200
    assert r.content_type == 'application/octet-stream'
    assert r.is_streamed is True
    assert sorted(r.json) == sorted(result)

    url = '/api/batch_%s?application=%s&format=avada' % (
        data_type, test_application_name)
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'Unrecognized "format"'


@mark.xparametrize
def test_batch_import(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, build_comments, inspect_comment, presented_tree,
        request_data, response_data, changed_tree, audit_data, last_audit_log,
        data_type):
    build_zk_tree((node['path'], node['value']) for node in presented_tree)
    build_comments(
        (node['path'], node.get('comment')) for node in presented_tree)

    if request_data['content'] is None:
        request_content = None
    else:
        request_content = [dict(r) for r in request_data['content']]
        for r in request_content:
            r.setdefault('application', test_application_name)
    request_file = io.BytesIO()
    json.dump(request_content, request_file)
    request_file.seek(0)

    url = '/api/batch_%s' % data_type
    headers = {'Authorization': test_application_token}
    data = {
        'overwrite': request_data['override'],
        'import_file': (request_file, 'some.json')
    }
    r = client.post(url, headers=headers, data=data)
    assert_response_ok(r)
    assert r.json['data'] == response_data

    for node in changed_tree:
        path = '/huskar/%s/%s%s' % (
            data_type, test_application_name, node['path'])
        assert zk.exists(path.strip('/'))
        assert zk.get(path.strip('/'))[0] == node['value'].encode('utf-8')
        cluster, key = node['path'].strip('/').split('/')
        assert inspect_comment(node['path']) == node.get('comment', '')

    audit_log = last_audit_log()
    nested = audit_data['nested'] % {'name': test_application_name}
    assert audit_log.action_name == 'IMPORT_%s' % data_type.upper()
    assert json.dumps(
        audit_log.action_json['data']['nested'], sort_keys=True,
        ensure_ascii=False) == nested
    assert audit_log.action_json['stored'] == audit_data['stored']
    assert audit_log.action_json['overwrite'] == audit_data['overwrite']
    assert audit_log.action_json['affected'] == audit_data['affected']
    assert audit_log.action_json['application_names'] == \
        list(json.loads(nested))


def test_batch_export_without_permission(
        client, test_application_name, test_token, data_type):
    stolen_application_token = test_token  # We need a non-application token

    url = '/api/batch_%s?application=%s' % (data_type, test_application_name)
    headers = {'Authorization': stolen_application_token}
    r = client.get(url, headers=headers)
    if data_type == 'switch':
        assert_response_ok(r)
    else:
        assert r.status_code == 400
        assert r.json['status'] == 'NoAuthError'
        assert 'has no read authority' in r.json['message']
        assert r.json['data'] is None


def test_batch_import_without_permission(
        client, test_application_name, stolen_application_token, zk,
        test_token, data_type):
    request_file = io.BytesIO()
    json.dump([{
        'application': test_application_name,
        'cluster': 'beta',
        'key': 'foo',
        'value': 'bar',
    }], request_file)
    request_file.seek(0)

    url = '/api/batch_%s' % data_type
    headers = {'Authorization': stolen_application_token}
    data = {'import_file': (request_file, 'some.json')}
    r = client.post(url, headers=headers, data=data)
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert 'has no write authority' in r.json['message']
    assert r.json['data'] is None

    path = '/huskar/%s/%s' % (data_type, test_application_name)
    assert not zk.exists(path.strip('/'))


def test_batch_import_with_cluster_duplicated_prefix(
        client, test_application_name, zk,
        test_application_token, data_type):
    request_file = io.BytesIO()
    payload = [{
        'application': test_application_name,
        'cluster': 'alta1-alta1-stable',
        'key': 'foo',
        'value': 'bar',
    }]
    json.dump(payload, request_file)
    request_file.seek(0)

    url = '/api/batch_%s' % data_type
    headers = {'Authorization': test_application_token}
    data = {'import_file': (request_file, 'some.json')}
    r = client.post(url, headers=headers, data=data)
    assert r.status_code == 400
    assert r.json['status'] == 'DuplicatedEZonePrefixError'
    assert r.json['message'] == \
        'Cluster name should not contain duplicated E-Zone prefix.'
    assert r.json['data'] is None


@mark.parametrize('exist', [False, True])
@mark.parametrize('key,prefix', [
    ('FX_foo', 'FX_'),
    ('HUSKAR233', 'HUSKAR'),
])
def test_batch_import_with_config_key_prefix_blacklisted(
        client, test_application_name, zk, key, prefix, exist,
        test_application_token, data_type, mocker):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_CONFIG_PREFIX_BLACKLIST:
            return True
        return default
    mocker.patch.object(settings, 'CONFIG_PREFIX_BLACKLIST', ['HUSKAR', 'FX_'])
    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    key = '{}.{}'.format(key, time.time())
    if exist:
        zk.ensure_path(
            '/huskar/{}/{}/alta1-stable/{}'.format(
                data_type, test_application_name, key))
    request_file = io.BytesIO()
    payload = [{
        'application': test_application_name,
        'cluster': 'alta1-stable',
        'key': key,
        'value': 'bar',
    }]
    json.dump(payload, request_file)
    request_file.seek(0)

    url = '/api/batch_%s' % data_type
    headers = {'Authorization': test_application_token}
    data = {'import_file': (request_file, 'some.json')}
    r = client.post(url, headers=headers, data=data)
    if exist or data_type != 'config':
        assert_response_ok(r)
        return

    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == (
        'The key {key} starts with {prefix} is denied.'.format(
            key=key, prefix=prefix))
    assert r.json['data'] is None


@mark.xparametrize
def test_batch_config_with_merge(
        client, zk, test_application_name, test_application_token,
        build_zk_tree, build_comments, minimal_mode, tree, result, data_type):
    if data_type != 'config':
        return

    build_zk_tree((node['path'], node['value']) for node in tree)
    if minimal_mode:
        zk.ensure_path('/huskar/config/%s' % test_application_name)
        sleep(0.1)
    else:
        build_comments((node['path'], node.get('comment')) for node in tree)

    result = [dict(r) for r in result]
    for r in result:
        r.setdefault('application', test_application_name)
    if minimal_mode:
        for r in result:
            r.pop('comment', None)

    url = (
        '/api/batch_config?application=%s&cluster=direct'
        % test_application_name)
    headers = {
        'Authorization': test_application_token,
        'X-Cluster-Name': 'beta',
    }
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert all(item['meta']['version'] > -1 for item in r.json['data'])
    assert sorted(
        {k: v for k, v in item.iteritems() if k != 'meta'}
        for item in r.json['data']
    ) == sorted(result)


def test_audit_log_with_large_instance(
        client, faker, mocker, test_application_name, test_application_token,
        data_type):
    logger = mocker.patch('huskar_api.api.utils.logger', autospec=True)
    data = {'key': faker.uuid4()[:8], 'value': 'x' * 65536}
    headers = {'Authorization': test_application_token}
    r = client.post('/api/config/%s/overall' % test_application_name,
                    data=data, headers=headers)
    assert_response_ok(r)
    assert logger.info.call_count == 1


def test_audit_log_with_broken_database(
        client, faker, mocker, test_application_name, test_application_token,
        data_type):
    logger = mocker.patch(
        'huskar_api.api.utils.fallback_audit_logger', autospec=True)
    session = mocker.patch('huskar_api.models.audit.audit.DBSession')
    session.side_effect = OperationalError(None, None, None, None)
    data = {'key': faker.uuid4()[:8], 'value': 'value'}
    headers = {'Authorization': test_application_token}
    r = client.post('/api/config/%s/overall' % test_application_name,
                    data=data, headers=headers)
    assert_response_ok(r)
    assert logger.info.call_count == 1


@mark.parametrize('key', [
    'FX_DATABASE_SETTINGS',
    'FX_REDIS_SETTINGS',
    'FX_AMQP_SETTINGS',
])
def test_add_config_with_reserved_key(
        client, zk, test_application_name, test_application_token,
        minimal_mode, key, data_type):
    cluster = 'bar'
    data = {'key': key, 'value': 'foo'}
    for method in 'POST', 'PUT':
        url = '/api/%s/%s/%s' % ('config', test_application_name, cluster)
        headers = {'Authorization': test_application_token}
        r = client.open(url, method=method, data=data, headers=headers)
        assert r.status_code == 400
        assert r.json['status'] == 'BadRequest'
        assert r.json['message'] == 'The key %s is reserved.' % key
        assert r.json['data'] is None


@mark.parametrize('key', [
    'FX_DATABASE_SETTINGS',
    'FX_REDIS_SETTINGS',
    'FX_AMQP_SETTINGS',
])
def test_batch_import_config_with_reserved_key(
        client, zk, test_application_name, test_application_token,
        minimal_mode, key, data_type):
    request_file = io.BytesIO()
    payload = [{
        'application': test_application_name,
        'cluster': 'bar',
        'key': key,
        'value': 'foo',
    }]
    json.dump(payload, request_file)
    request_file.seek(0)

    url = '/api/batch_config'
    headers = {'Authorization': test_application_token}
    data = {'import_file': (request_file, 'some.json')}
    r = client.post(url, headers=headers, data=data)
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'The key %s is reserved.' % key
    assert r.json['data'] is None
