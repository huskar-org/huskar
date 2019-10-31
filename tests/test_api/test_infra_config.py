from __future__ import absolute_import

import json

from pytest import fixture, mark

from huskar_api import settings
from huskar_api.extras.email import EmailTemplate
from huskar_api.models import huskar_client
from huskar_api.models.auth import Application, Authority
from huskar_api.models.infra import InfraDownstream
from huskar_api.models.instance import InfraInfo
from ..utils import assert_response_ok


@fixture
def admin_infra_owner_emails(mocker):
    return mocker.patch.object(
        settings, 'ADMIN_INFRA_OWNER_EMAILS', {
            'redis': ['san.zhang@example.com']
        }
    )


@fixture
def test_application_name(test_application):
    return test_application.application_name


@fixture
def test_redis(test_team):
    application = Application.create(u'redis.test', test_team.id)
    return application.application_name


@fixture
def test_oss(test_team):
    application = Application.create(u'oss.hello_test', test_team.id)
    return application.application_name


@fixture
def infra_info(test_application_name):
    return InfraInfo(
        huskar_client.client, test_application_name, 'redis')


@fixture(params=[False, True])
def exception_on_recording_downstream(request, mocker):
    if request.param:
        mocker.patch.object(InfraDownstream, 'bind', side_effect=ValueError)
        mocker.patch.object(InfraDownstream, 'unbind', side_effect=ValueError)
    return request.param


def test_get_infra_config_ok(
        client, infra_info, test_application_name, test_application_token):
    infra_info.set_by_name('r100010', 'idcs', 'alta1', {
        'url': 'sam+redis://redis.test/overall.alta1'})
    infra_info.save()

    url = '/api/infra-config/%s/redis/r100010' % test_application_name
    r = client.get(url, headers={'Authorization': test_application_token})
    assert_response_ok(r)
    assert r.json['data'] == {'infra_config': [{
        'scope_type': 'idcs', 'scope_name': 'alta1',
        'value': {'url': 'sam+redis://redis.test/overall.alta1'},
    }]}


def test_get_infra_config_fail(
        client, test_application_name, test_application_token, test_token):
    url = '/api/infra-config/%s/redis/r100010' % test_application_name
    r = client.get(url, headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'

    url = '/api/infra-config/%s/dynamo/d100010' % test_application_name
    r = client.get(url, headers={'Authorization': test_application_token})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'Specified infra_type is not found'


@mark.parametrize('scope_type,scope_name', [
    ('clusters', u'altb1-channel-stable-1'),
    ('idcs', 'altb1'),
])
def test_put_infra_config_ok(
        zk, client, infra_info, test_application_name, test_application_token,
        test_redis, last_audit_log, mocker,
        scope_type, scope_name,
        exception_on_recording_downstream):
    major_application_name = test_application_name

    logger = mocker.patch(
        'huskar_api.api.middlewares.logger.logger', autospec=True)

    exit_scope_name = scope_name.replace('altb1', 'alta1')
    infra_info.set_by_name('r100010', scope_type, exit_scope_name, {
        'url': 'redis://localhost:6543'})
    infra_info.save()

    url = 'sam+redis://%s/overall.alta' % test_redis
    r = client.put(
        '/api/infra-config/%s/redis/r100010' % test_application_name,
        query_string={'scope_type': scope_type, 'scope_name': scope_name},
        data=json.dumps({'url': url}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)
    assert r.json['data'] == {'infra_config': [
        {'scope_type': scope_type, 'scope_name': exit_scope_name,
         'value': {'url': 'redis://localhost:6543'}},
        {'scope_type': scope_type, 'scope_name': scope_name,
         'value': {'url': url}},
    ]}
    request_args = json.loads(logger.mock_calls[0][1][-6])
    assert 'url' not in request_args

    InfraDownstream.flush_cache_by_application(test_redis)
    ds = InfraDownstream.get_multi_by_application(test_redis)
    if exception_on_recording_downstream:
        assert len(ds) == 0
    else:
        assert len(ds) == 1
        assert ds[0].user_application_name == major_application_name
        assert ds[0].user_infra_type == 'redis'
        assert ds[0].user_infra_name == 'r100010'

    audit_log = last_audit_log()
    assert audit_log and audit_log.action_name == 'UPDATE_INFRA_CONFIG'
    assert audit_log.action_json['application_name'] == major_application_name
    assert audit_log.action_json['infra_type'] == 'redis'
    assert audit_log.action_json['infra_name'] == 'r100010'
    assert audit_log.action_json['scope_type'] == scope_type
    assert audit_log.action_json['scope_name'] == scope_name
    assert audit_log.action_json['data']['new'] == {'url': url}
    assert audit_log.action_json['data']['old'] is None

    url = 'sam+redis://%s/overall.alta1' % test_redis
    r = client.put(
        '/api/infra-config/%s/redis/r100010' % test_application_name,
        query_string={'scope_type': scope_type, 'scope_name': scope_name},
        data=json.dumps({'url': url}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)


@mark.parametrize("method", ['put', 'patch'])
def test_put_patch_infra_config_fail(
        client, test_application_name, test_application_token, test_token,
        last_audit_log, method):
    url = '/api/infra-config/%s/redis/r100010' % test_application_name
    r = client.open(
        method=method, path=url,
        headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'

    r = client.open(
        method=method, path=url,
        headers={'Authorization': test_application_token},
        query_string={'scope_type': 'foo', 'scope_name': 'alta1'})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'Unrecognized scope_type "foo"'

    for data in [None, 'null', '""', '233', '[]']:
        url = '/api/infra-config/%s/redis/r100010' % test_application_name
        r = client.open(
            method=method, path=url,
            headers={'Authorization': test_application_token},
            query_string={'scope_type': 'idcs', 'scope_name': 'alta1'},
            data=data)
        assert r.status_code == 400
        assert r.json['status'] == 'BadRequest'
        assert r.json['message'] == 'Unacceptable content type or content body'

    r = client.open(
        method=method, path=url,
        headers={'Authorization': test_application_token},
        query_string={'scope_type': 'idcs', 'scope_name': 'tg1'})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'Unrecognized scope_name "tg1"'

    r = client.open(method=method, path=url,
                    headers={'Authorization': test_application_token},
                    query_string={'scope_type': 'clusters', 'scope_name': ''})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'Unrecognized scope_name ""'

    r = client.put(
        url, query_string={'scope_type': 'idcs', 'scope_name': 'altb1'},
        data=json.dumps({'url': 'sam+redis://redis.never_exist/overall'}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert r.status_code == 400
    assert r.json['status'] == 'ApplicationNotExistedError'
    assert r.json['message'] == "application: redis.never_exist doesn't exist"

    url = 'sam+redis://%s/overall.alta1' % test_redis
    r = client.patch(
        '/api/infra-config/%s/redis/r100011' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'alta1'},
        data=json.dumps({'url': url}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == "r100011 doesn't exist"

    assert last_audit_log() is None


def test_patch_infra_config_ok(
        client, infra_info, test_application_name, test_application_token,
        test_redis, last_audit_log, mocker):
    major_application_name = test_application_name

    logger = mocker.patch(
        'huskar_api.api.middlewares.logger.logger', autospec=True)

    infra_info.set_by_name('r100010', 'idcs', 'alta1', {
        'url': 'redis://localhost:6543',
        "max_pool_size": 100,
        "connect_timeout_ms": 5,
    })
    infra_info.save()

    url = 'sam+redis://%s/overall.alta' % test_redis
    r = client.patch(
        '/api/infra-config/%s/redis/r100010' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'alta1'},
        data=json.dumps({'connect_timeout_ms': 10, 'url': url}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)
    assert r.json['data'] == {'infra_config': [
        {'scope_type': 'idcs', 'scope_name': 'alta1',
         'value': {'url': url,
                   "max_pool_size": 100,
                   "connect_timeout_ms": 10,
                   }
         },
    ]}
    request_args = json.loads(logger.mock_calls[0][1][-6])
    assert 'url' not in request_args

    audit_log = last_audit_log()
    assert audit_log and audit_log.action_name == 'UPDATE_INFRA_CONFIG'
    assert audit_log.action_json['application_name'] == major_application_name
    assert audit_log.action_json['infra_type'] == 'redis'
    assert audit_log.action_json['infra_name'] == 'r100010'
    assert audit_log.action_json['scope_type'] == 'idcs'
    assert audit_log.action_json['scope_name'] == 'alta1'
    assert audit_log.action_json['data']['new'] == {
        'url': url,
        'max_pool_size': 100,
        'connect_timeout_ms': 10,
    }
    assert audit_log.action_json['data']['old'] == {
        'url': 'redis://localhost:6543',
        'max_pool_size': 100,
        'connect_timeout_ms': 5,
    }

    url = 'sam+redis://%s/overall.alta1' % test_redis
    r = client.patch(
        '/api/infra-config/%s/redis/r100010' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'alta1'},
        data=json.dumps({'url': url}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)


def test_delete_infra_config_ok(
        client, db, infra_info, test_application_name, test_application_token,
        last_audit_log):
    major_application_name = test_application_name
    infra_info.set_by_name('r100010', 'idcs', 'alta1', {
        'url': 'sam+redis://redis.100010/overall.alta1'})
    infra_info.set_by_name('r100010', 'idcs', 'altb1', {
        'url': 'sam+redis://redis.100010/overall.altb1'})
    infra_info.save()

    InfraDownstream.bindmany() \
        .bind(major_application_name, 'redis', 'r100010', 'idcs', 'alta1',
              'url', 'redis.100010') \
        .bind(major_application_name, 'redis', 'r100010', 'idcs', 'altb1',
              'url', 'redis.100010') \
        .commit()

    InfraDownstream.flush_cache_by_application('redis.100010')
    ds = InfraDownstream.get_multi_by_application('redis.100010')
    assert len(ds) == 2

    r = client.delete(
        '/api/infra-config/%s/redis/r100010' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'altb1'},
        headers={'Authorization': test_application_token})
    assert_response_ok(r)
    assert r.json['data'] == {'infra_config': [
        {'scope_type': 'idcs', 'scope_name': 'alta1',
         'value': {'url': 'sam+redis://redis.100010/overall.alta1'}},
    ]}

    InfraDownstream.flush_cache_by_application('redis.100010')
    ds = InfraDownstream.get_multi_by_application('redis.100010')
    assert len(ds) == 1
    assert ds[0].user_application_name == major_application_name
    assert ds[0].user_infra_type == 'redis'
    assert ds[0].user_infra_name == 'r100010'

    r = client.delete(
        '/api/infra-config/%s/redis/r100010' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'alta1'},
        headers={'Authorization': test_application_token})
    assert_response_ok(r)
    assert r.json['data'] == {'infra_config': []}

    InfraDownstream.flush_cache_by_application('redis.100010')
    ds = InfraDownstream.get_multi_by_application('redis.100010')
    assert len(ds) == 0

    audit_log = last_audit_log()
    assert audit_log and audit_log.action_name == 'DELETE_INFRA_CONFIG'
    assert audit_log.action_json['application_name'] == major_application_name
    assert audit_log.action_json['infra_type'] == 'redis'
    assert audit_log.action_json['infra_name'] == 'r100010'
    assert audit_log.action_json['scope_type'] == 'idcs'
    assert audit_log.action_json['scope_name'] == 'alta1'
    assert audit_log.action_json['data']['new'] is None
    assert audit_log.action_json['data']['old'] == {
        'url': 'sam+redis://redis.100010/overall.alta1'}


def test_delete_infra_config_fail(
        client, test_application_name, test_application_token, test_token,
        last_audit_log):
    url = '/api/infra-config/%s/redis/r100010' % test_application_name
    r = client.delete(url, headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'

    r = client.delete(
        url, headers={'Authorization': test_application_token},
        query_string={'scope_type': 'foo', 'scope_name': 'alta1'})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'Unrecognized scope_type "foo"'

    r = client.put(
        url, headers={'Authorization': test_application_token},
        query_string={'scope_type': 'idcs', 'scope_name': 'tg1'})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'Unrecognized scope_name "tg1"'

    r = client.put(
        url, headers={'Authorization': test_application_token},
        query_string={'scope_type': 'clusters', 'scope_name': ''})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'Unrecognized scope_name ""'

    assert last_audit_log() is None


def test_put_infra_config_with_email_notification(
        client, infra_info, test_application, test_application_name,
        test_application_token, test_user, test_redis,
        last_audit_log, mocker, admin_infra_owner_emails):
    major_application_name = test_application_name

    deliver_email_safe = mocker.patch(
        'huskar_api.api.infra_config.deliver_email_safe', autospec=True)

    # 1st - should deliver email
    url_altb = 'sam+redis://%s/overall.altb1' % test_redis
    r = client.put(
        '/api/infra-config/%s/redis/r100010' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'altb1',
                      'owner_mail': 'san.zhang@example.com'},
        data=json.dumps({'url': url_altb}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)
    assert r.json['data'] == {'infra_config': [
        {'scope_type': 'idcs', 'scope_name': 'altb1', 'value':
            {'url': url_altb}},
    ]}

    deliver_email_safe.assert_called_once_with(
        EmailTemplate.INFRA_CONFIG_CREATE, 'san.zhang@example.com', {
            'application_name': major_application_name,
            'infra_type': 'redis',
            'infra_name': 'r100010',
            'is_authorized': False,
        }, cc=['san.zhang@example.com']
    )

    deliver_email_safe.reset_mock()

    # 3rd - should not deliver email
    url_alta = 'sam+redis://%s/overall.alta1' % test_redis
    r = client.put(
        '/api/infra-config/%s/redis/r100010' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'alta1',
                      'owner_mail': 'san.zhang@example.com'},
        data=json.dumps({'url': url_alta}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)
    assert r.json['data'] == {'infra_config': [
        {'scope_type': 'idcs', 'scope_name': 'alta1', 'value':
            {'url': url_alta}},
        {'scope_type': 'idcs', 'scope_name': 'altb1', 'value':
            {'url': url_altb}},
    ]}

    assert not deliver_email_safe.called

    deliver_email_safe.reset_mock()

    # 3nd - should deliver email (with new code name) with existed user
    r = client.put(
        '/api/infra-config/%s/redis/r100011' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'alta1',
                      'owner_mail': test_user.email},
        data=json.dumps({'url': 'sam+redis://%s/overall.alta' % test_redis}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)

    deliver_email_safe.assert_called_once_with(
        EmailTemplate.INFRA_CONFIG_CREATE, test_user.email, {
            'application_name': major_application_name,
            'infra_type': 'redis',
            'infra_name': 'r100011',
            'is_authorized': False,
        }, cc=['san.zhang@example.com']
    )

    deliver_email_safe.reset_mock()

    # 4th - should deliver email (with new code name) with granted user
    test_application.ensure_auth(Authority.READ, test_user.id)
    r = client.put(
        '/api/infra-config/%s/redis/r100012' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'alta1',
                      'owner_mail': test_user.email},
        data=json.dumps({'url': 'sam+redis://%s/overall.alta' % test_redis}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)

    deliver_email_safe.assert_called_once_with(
        EmailTemplate.INFRA_CONFIG_CREATE, test_user.email, {
            'application_name': major_application_name,
            'infra_type': 'redis',
            'infra_name': 'r100012',
            'is_authorized': True,
        }, cc=['san.zhang@example.com']
    )


def test_new_oss_infra_config(
        client, test_application_name, test_application_token,
        test_oss, last_audit_log, mocker):
    major_application_name = test_application_name
    infra_info = InfraInfo(
        huskar_client.client, test_application_name, 'oss')

    logger = mocker.patch(
        'huskar_api.api.middlewares.logger.logger', autospec=True)

    infra_info.set_by_name('r100010', 'idcs', 'alta1', {
        'url': 'http://localhost:233',
        'max_pool_size': 100,
        'connect_timeout_ms': 5,
        'max_error_retry': 3,
    })
    infra_info.save()

    url = 'sam+http://user:passwd@%s:666/overall.alta' % test_oss
    r = client.patch(
        '/api/infra-config/%s/oss/r100010' % test_application_name,
        query_string={'scope_type': 'idcs', 'scope_name': 'alta1'},
        data=json.dumps({'connect_timeout_ms': 10, 'url': url,
                         'idle_timeout_ms': 5000}),
        headers={'Authorization': test_application_token,
                 'Content-Type': 'application/json'})
    assert_response_ok(r)
    assert r.json['data'] == {'infra_config': [
        {'scope_type': 'idcs', 'scope_name': 'alta1',
         'value': {
             'url': url,
             'max_pool_size': 100,
             'connect_timeout_ms': 10,
             'idle_timeout_ms': 5000,
             'max_error_retry': 3}
         },
    ]}
    request_args = json.loads(logger.mock_calls[0][1][-6])
    assert 'url' not in request_args

    audit_log = last_audit_log()
    assert audit_log and audit_log.action_name == 'UPDATE_INFRA_CONFIG'
    assert audit_log.action_json['application_name'] == major_application_name
    assert audit_log.action_json['infra_type'] == 'oss'
    assert audit_log.action_json['infra_name'] == 'r100010'
    assert audit_log.action_json['scope_type'] == 'idcs'
    assert audit_log.action_json['scope_name'] == 'alta1'
    assert audit_log.action_json['data']['new'] == {
        'url': url,
        'max_pool_size': 100,
        'connect_timeout_ms': 10,
        'idle_timeout_ms': 5000,
        'max_error_retry': 3,
    }
    assert audit_log.action_json['data']['old'] == {
        'url': 'http://localhost:233',
        'max_pool_size': 100,
        'connect_timeout_ms': 5,
        'max_error_retry': 3,
    }
