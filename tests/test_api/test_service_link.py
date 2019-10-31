from __future__ import absolute_import

import json

import kazoo.exceptions
import pytest

from ..utils import assert_response_ok


@pytest.fixture
def add_service(zk, test_application):
    def factory(cluster, key, value):
        if not key or not value:
            return
        path = '/huskar/service/%s/%s/%s' % (
            test_application.application_name, cluster, key)
        return zk.create(path, value, makepath=True)
    return factory


@pytest.fixture
def get_service(zk, test_application):
    def factory(cluster, key):
        path = '/huskar/service/%s/%s/%s' % (
            test_application.application_name, cluster, key)
        return zk.get(path)
    return factory


@pytest.fixture
def add_servicelink(zk, test_application):
    def factory(symlink_cluster, physical_cluster):
        path = '/huskar/service/%s/%s' % (
            test_application.application_name, symlink_cluster)
        data = json.dumps({'link': [physical_cluster]})
        if zk.exists(path):
            return zk.set(path, data)
        else:
            return zk.create(path, data, makepath=True)
    return factory


@pytest.fixture
def get_servicelink(zk, test_application):
    def factory(cluster):
        path = '/huskar/service/%s/%s' % (
            test_application.application_name, cluster)
        try:
            data, stat = zk.get(path)
        except kazoo.exceptions.NoNodeError:
            return None, None
        if not data:
            return None, None
        data = json.loads(data)
        assert data.pop('_version', None) in ('1', None)
        return data, stat
    return factory


@pytest.mark.xparametrize
def test_link_cluster(client, test_application, test_application_token,
                      last_audit_log, add_service, get_servicelink,
                      physical_cluster, physical_key, physical_value,
                      symlink_cluster, symlink_key, symlink_value,
                      webhook_backends):
    add_service(physical_cluster, physical_key, physical_value)
    add_service(symlink_cluster, symlink_key, symlink_value)

    url = '/api/servicelink/%s/%s' % (
        test_application.application_name, symlink_cluster)
    headers = {'Authorization': test_application_token}
    r = client.post(url, data={'link': physical_cluster}, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] is None

    data, stat = get_servicelink(symlink_cluster)
    assert data == {'link': [physical_cluster]}
    assert stat.version in (0, 1)

    audit_log = last_audit_log()
    assert audit_log.action_name == 'ASSIGN_CLUSTER_LINK'
    assert audit_log.action_json['application_name'] == \
        test_application.application_name
    assert audit_log.action_json['cluster_name'] == symlink_cluster
    assert audit_log.action_json['physical_name'] == physical_cluster

    for result in webhook_backends:
        assert result['action_name'] == 'ASSIGN_CLUSTER_LINK'
        assert result['action_data']['application_name'] == \
            test_application.application_name
        assert result['action_data']['cluster_name'] == symlink_cluster
        assert result['action_data']['physical_name'] == physical_cluster


@pytest.mark.xparametrize
def test_link_cluster_on_dirty_node(
        client, zk, test_application, test_application_token, add_service,
        get_servicelink, symlink_cluster, physical_cluster, dirty_data):
    zk.create('/huskar/service/%s/%s' % (
        test_application.application_name, symlink_cluster
    ), dirty_data, makepath=True)
    add_service(physical_cluster, 'key', 'value')

    url = '/api/servicelink/%s/%s' % (
        test_application.application_name, symlink_cluster)
    headers = {'Authorization': test_application_token}
    r = client.post(url, data={'link': physical_cluster}, headers=headers)
    assert_response_ok(r)

    data, stat = get_servicelink(symlink_cluster)
    assert data == {'link': [physical_cluster]}
    assert stat.version == 1, 'should be modified once'


@pytest.mark.xparametrize
def test_link_cluster_failed(client, test_application, test_application_token,
                             last_audit_log, add_service, get_servicelink,
                             add_servicelink, physical_cluster, physical_key,
                             physical_value, symlink_cluster, symlink_key,
                             symlink_value, present_link, error_name,
                             error_reason):
    add_service(physical_cluster, physical_key, physical_value)
    add_service(symlink_cluster, symlink_key, symlink_value)
    if present_link:
        add_servicelink(present_link['from'], present_link['to'])

    url = '/api/servicelink/%s/%s' % (
        test_application.application_name, symlink_cluster)
    headers = {'Authorization': test_application_token}
    r = client.post(url, data={'link': physical_cluster}, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == error_name
    assert r.json['message'] == error_reason
    assert r.json['data'] is None

    if (not present_link or
            present_link['from'] != symlink_cluster or
            present_link['to'] != physical_cluster):
        assert get_servicelink(symlink_cluster) == (None, None)

    assert last_audit_log() is None


@pytest.mark.xparametrize
def test_unlink_cluster(client, test_application, test_application_token,
                        last_audit_log, add_service, get_service,
                        add_servicelink, get_servicelink,
                        physical_cluster, physical_key, physical_value,
                        symlink_cluster, symlink_key, symlink_value,
                        symlink_version):
    add_service(physical_cluster, physical_key, physical_value)
    add_service(symlink_cluster, symlink_key, symlink_value)
    add_servicelink(symlink_cluster, physical_cluster)

    url = '/api/servicelink/%s/%s' % (
        test_application.application_name, symlink_cluster)
    headers = {'Authorization': test_application_token}
    r = client.delete(url, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] is None

    data, stat = get_servicelink(symlink_cluster)
    assert data == {'link': []}
    assert stat.version == symlink_version

    if symlink_key and symlink_value:
        data, stat = get_service(symlink_cluster, symlink_key)
        assert data == symlink_value, 'original content should be present'
        assert stat.version == 0, 'original content should not be changed'

    audit_log = last_audit_log()
    assert audit_log.action_name == 'DELETE_CLUSTER_LINK'
    assert audit_log.action_json['application_name'] == \
        test_application.application_name
    assert audit_log.action_json['cluster_name'] == symlink_cluster
    assert audit_log.action_json['physical_name'] == physical_cluster


@pytest.mark.xparametrize
def test_get_cluster_link(client, test_application, test_application_token,
                          add_service, add_servicelink,
                          physical_cluster, physical_key, physical_value,
                          symlink_cluster, symlink_key, symlink_value):
    add_service(physical_cluster, physical_key, physical_value)
    add_service(symlink_cluster, symlink_key, symlink_value)
    add_servicelink(symlink_cluster, physical_cluster)

    url = '/api/servicelink/%s/%s' % (
        test_application.application_name, symlink_cluster)
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] == physical_cluster


@pytest.mark.xparametrize
def test_get_empty_cluster_link(client, test_application,
                                test_application_token, add_service,
                                symlink_cluster, symlink_key, symlink_value):
    add_service(symlink_cluster, symlink_key, symlink_value)

    url = '/api/servicelink/%s/%s' % (
        test_application.application_name, symlink_cluster)
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] is None


@pytest.mark.xparametrize
def test_clear_cluster(client, test_application, test_application_token,
                       add_service, add_servicelink, get_servicelink,
                       cluster, key, value, linkto):
    """See also http://jira.ele.to:8088/browse/FXBUG-872."""
    add_servicelink(cluster, linkto)
    data, _ = get_servicelink(cluster)
    assert data == {'link': [linkto]}

    for c in cluster, linkto:
        add_service(c, key, value)

        url = '/api/service/%s/%s' % (test_application.application_name, c)
        headers = {'Authorization': test_application_token}
        r = client.delete(url, data={'key': key}, headers=headers)
        assert_response_ok(r)
        assert r.json['data'] is None

    data, _ = get_servicelink(cluster)
    assert data == {'link': [linkto]}
