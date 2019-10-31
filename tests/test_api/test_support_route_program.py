from __future__ import absolute_import

import json

from kazoo.exceptions import NoNodeError
from pytest import fixture, raises, mark
from huskar_sdk_v2.consts import OVERALL

from huskar_api.models.exceptions import OutOfSyncError
from huskar_api.models.instance.schema import Instance
from ..utils import assert_response_ok


@fixture
def hijack_url():
    return '/api/_internal/arch/route-program'


@fixture
def hijack_list(zk):
    path_format = '/huskar/config/arch.huskar_api/{}/ROUTE_HIJACK_LIST'

    def update(cluster_name, data):
        path = path_format.format(cluster_name)
        zk.ensure_path(path)
        zk.set(path, json.dumps(data))
        return data

    def reset(cluster_name):
        path = path_format.format(cluster_name)
        zk.ensure_path(path)
        zk.delete(path)

    def get(cluster_name):
        path = path_format.format(cluster_name)
        return json.loads(zk.get(path)[0])

    update.reset = reset
    update.get = get

    return update


@mark.parametrize('cluster_name', [OVERALL, 'altb1-channel-stable-1'])
def test_get_list(client, test_token, hijack_url, hijack_list, cluster_name):
    hijack_list.reset(OVERALL)
    hijack_list.reset(cluster_name)

    r = client.get(hijack_url, headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.json['data'] == {'route_stage': {}}

    hijack_list(OVERALL, {'foo.test': 'E'})
    hijack_list(cluster_name, {'foo.test': 'C'})

    r = client.get(
        hijack_url, headers={'Authorization': test_token},
        query_string={'cluster': cluster_name})
    assert_response_ok(r)
    assert r.json['data'] == {'route_stage': {'foo.test': 'C'}}


@mark.parametrize('cluster_name', [OVERALL, 'altb1-channel-stable-1'])
def test_update_list(
        client, test_application, test_application_token, admin_token,
        hijack_url, hijack_list, last_audit_log, cluster_name):
    hijack_list.reset(cluster_name)

    with raises(NoNodeError):
        hijack_list.get(cluster_name)

    r = client.post(hijack_url, data={
        'application': test_application.application_name,
        'stage': 'E',
        'cluster': cluster_name
    }, headers={'Authorization': test_application_token})
    assert_response_ok(r)
    assert hijack_list.get(cluster_name) == \
        {test_application.application_name: 'E'}
    audit_log = last_audit_log()
    assert audit_log.action_name == 'PROGRAM_UPDATE_ROUTE_STAGE'
    assert audit_log.action_json['application_name'] == \
        test_application.application_name
    assert audit_log.action_json['old_stage'] == 'D'
    assert audit_log.action_json['new_stage'] == 'E'

    r = client.post(hijack_url, data={
        'application': test_application.application_name,
        'stage': 'S',
        'cluster': cluster_name,
    }, headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert hijack_list.get(cluster_name) == \
        {test_application.application_name: 'S'}
    audit_log = last_audit_log()
    assert audit_log.action_name == 'PROGRAM_UPDATE_ROUTE_STAGE'
    assert audit_log.action_json['application_name'] == \
        test_application.application_name
    assert audit_log.action_json['old_stage'] == 'E'
    assert audit_log.action_json['new_stage'] == 'S'


@mark.parametrize('cluster_name', [OVERALL, 'altb1-channel-stable-1'])
def test_update_list_with_invalid_stage(
        client, test_application, admin_token, hijack_url, hijack_list,
        last_audit_log, cluster_name):
    hijack_list.reset(cluster_name)
    hijack_list(cluster_name, {test_application.application_name: 'S'})

    r = client.post(hijack_url, data={
        'application': test_application.application_name,
        'stage': 'V',
        'cluster': cluster_name
    }, headers={'Authorization': admin_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'stage is invalid'
    assert hijack_list.get(cluster_name) == \
        {test_application.application_name: 'S'}
    assert last_audit_log() is None


def test_update_list_with_invalid_cluster(
        client, test_application, admin_token, hijack_url, hijack_list,
        last_audit_log):
    r = client.post(hijack_url, data={
        'application': test_application.application_name,
        'stage': 'E',
        'cluster': 'foo'
    }, headers={'Authorization': admin_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'cluster is invalid'
    with raises(NoNodeError):
        hijack_list.get('foo')
    assert last_audit_log() is None


@mark.parametrize('cluster_name', [OVERALL, 'altb1-channel-stable-1'])
def test_update_list_with_same_stage(
        client, test_application, admin_token, hijack_url, hijack_list,
        last_audit_log, cluster_name):
    hijack_list.reset(cluster_name)
    hijack_list(cluster_name, {test_application.application_name: 'E'})

    r = client.post(hijack_url, data={
        'application': test_application.application_name,
        'stage': 'E',
        'cluster': cluster_name
    }, headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert last_audit_log() is None
    assert hijack_list.get(cluster_name) == \
        {test_application.application_name: 'E'}


def test_add_list_oos(
        mocker, client, test_application, test_application_token, hijack_url):
    mocker.patch.object(Instance, 'save', side_effect=OutOfSyncError())

    r = client.post(hijack_url, data={
        'application': test_application.application_name,
        'stage': 'D',
    }, headers={'Authorization': test_application_token})
    assert r.status_code == 409, r.data
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == 'resource is modified by another request'
