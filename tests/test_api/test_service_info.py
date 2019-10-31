from __future__ import absolute_import

import json

import pytest
from kazoo.exceptions import NoNodeError

from ..utils import assert_response_ok


def setup_zk(presented_data, test_application, zk):
    path = presented_data['path'] % test_application.application_name
    if presented_data['data'] is False:
        try:
            zk.delete(path, recursive=True)
        except NoNodeError:
            pass
    else:
        if zk.exists(path):
            zk.set(path, presented_data['data'])
        else:
            zk.create(path, presented_data['data'], makepath=True)


@pytest.mark.xparametrize
def test_get_service_info(client, test_application, test_application_token,
                          zk, presented_data, expected_data):
    setup_zk(presented_data, test_application, zk)

    url = '/api/serviceinfo/%s' % test_application.application_name
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert r.status_code == expected_data['status_code']
    assert r.json == expected_data['body']


@pytest.mark.xparametrize
def test_get_cluster_info(client, test_application, test_application_token,
                          zk, presented_data, expected_data):
    setup_zk(presented_data, test_application, zk)

    url = '/api/serviceinfo/%s/beta' % test_application.application_name
    headers = {'Authorization': test_application_token}
    r = client.get(url, headers=headers)
    assert r.status_code == expected_data['status_code']
    assert r.json == expected_data['body']


@pytest.mark.xparametrize
def test_put_service_info(client, test_application, test_application_token,
                          zk, presented_data, request_data, expected_data):
    setup_zk(presented_data, test_application, zk)

    url = '/api/serviceinfo/%s' % test_application.application_name
    headers = {'Authorization': test_application_token,
               'Content-Type': 'application/json'}
    r = client.put(url, headers=headers, data=json.dumps(request_data))
    assert r.status_code == expected_data['status_code']
    assert r.json == expected_data['body']

    data, _ = zk.get(
        presented_data['path'] % test_application.application_name)
    assert json.loads(data) == expected_data['znode']


@pytest.mark.xparametrize
def test_put_cluster_info(client, test_application, test_application_token,
                          zk, presented_data, request_data, expected_data,
                          last_audit_log, webhook_backends):
    setup_zk(presented_data, test_application, zk)

    url = '/api/serviceinfo/%s/stable' % test_application.application_name
    headers = {'Authorization': test_application_token,
               'Content-Type': 'application/json'}
    r = client.put(url, headers=headers, data=json.dumps(request_data))
    assert r.status_code == expected_data['status_code']
    assert r.json == expected_data['body']

    data, _ = zk.get(
        presented_data['path'] % test_application.application_name)
    assert json.loads(data) == expected_data['znode']

    if r.status_code == 200:
        new_data = dict(expected_data['znode']).pop('info')
        audit_log = last_audit_log()
        assert audit_log.action_json['data']['new'] == new_data

        for result in webhook_backends:
            assert result['action_name'] == 'UPDATE_CLUSTER_INFO'
            assert result['action_data']['application_name'] == \
                test_application.application_name


@pytest.mark.xparametrize
def test_delete_service_info(client, test_application, test_application_token,
                             zk, presented_data, expected_data):
    setup_zk(presented_data, test_application, zk)

    url = '/api/serviceinfo/%s' % test_application.application_name
    headers = {'Authorization': test_application_token}
    r = client.delete(url, headers=headers)
    assert_response_ok(r)

    data, _ = zk.get(
        presented_data['path'] % test_application.application_name)
    assert json.loads(data) == expected_data['znode']


@pytest.mark.xparametrize
def test_delete_cluster_info(client, test_application, test_application_token,
                             zk, presented_data, expected_data,
                             last_audit_log):
    setup_zk(presented_data, test_application, zk)

    url = '/api/serviceinfo/%s/beta' % test_application.application_name
    headers = {'Authorization': test_application_token}
    r = client.delete(url, headers=headers)
    assert_response_ok(r)

    data, _ = zk.get(
        presented_data['path'] % test_application.application_name)
    assert json.loads(data) == expected_data['znode']

    audit_log = last_audit_log()
    assert audit_log.action_name == 'UPDATE_CLUSTER_INFO'
    assert audit_log.action_json['data']['new'] == {}


@pytest.mark.xparametrize('test_put_service_info')
def test_put_service_info_no_permitted(client, test_application, zk,
                                       stolen_application_token,
                                       presented_data, request_data,
                                       expected_data):
    setup_zk(presented_data, test_application, zk)

    url = '/api/serviceinfo/%s/stable' % test_application.application_name
    headers = {'Authorization': stolen_application_token,
               'Content-Type': 'application/json'}
    r = client.put(url, headers=headers, data=json.dumps(request_data))
    assert r.status_code == 400, r.data  # TODO should be 401
    assert r.json['status'] == 'NoAuthError'
    assert r.json['message'] == (
        'stolen-{0} has no write authority on {0}'.format(
            test_application.application_name
        )
    )
    assert r.json['data'] is None

    if presented_data['data'] is False:
        with pytest.raises(NoNodeError):
            zk.get(presented_data['path'] % test_application.application_name)
    else:
        data, _ = zk.get(
            presented_data['path'] % test_application.application_name)
        assert data == presented_data['data'], 'nothing changed'
