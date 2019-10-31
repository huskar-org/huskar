from __future__ import absolute_import

from copy import deepcopy

from gevent import sleep
from pytest import mark, fixture

from huskar_api import settings
from huskar_api.switch import (
    switch, SWITCH_DISABLE_FETCH_VIA_API, SWITCH_DISABLE_UPDATE_VIA_API)
from huskar_api.models.auth import Authority


@fixture
def build_zk_tree(zk, test_application):
    test_application_name = test_application.application_name

    def factory(nodes):
        for path, value in nodes:
            path = '/huskar/config/%s%s' % (test_application_name, path)
            value = value.encode('utf-8')
            zk.create(path.strip('/'), value, makepath=True)
    return factory


@mark.xparametrize
def test_fetch_via_api(
        mocker, client, test_token, switch_on, test_application,
        test_application_token, zk, minimal_mode,
        allow_users, path, build_zk_tree, allow, is_fe):
    def fake_switch(name, default=True):
        if name == SWITCH_DISABLE_FETCH_VIA_API:
            return switch_on
        return default

    test_application_name = test_application.application_name
    for endpoint, users in allow_users.items():
        if '{application}' in users:
            users.append(test_application_name)
    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    mocker.patch.object(settings, 'ALLOW_FETCH_VIA_API_USERS', allow_users)

    build_zk_tree([('/test_cluster', '233')])
    if minimal_mode:
        zk.ensure_path('/huskar/config/%s' % test_application_name)
        sleep(0.1)

    url = path.format({'application': test_application.application_name})
    headers = {'Authorization': test_application_token}
    if is_fe:
        headers['X-Frontend-Name'] = 'arch.huskar_fe'
        headers['Authorization'] = test_token
    r = client.get(url, headers=headers)

    if allow:
        assert r.status_code in [404, 200]
    else:
        msg = (
            'Request this api is forbidden, '
            'please access huskar console instead')
        assert r.status_code == 403
        assert r.json['message'] == msg


@mark.xparametrize
def test_update_via_api(
        mocker, client, switch_on, test_token,
        allow_users, path, build_zk_tree, test_user,
        test_application, test_application_token, data, is_fe,
        status_code, message):
    def fake_switch(name, default=True):
        if name == SWITCH_DISABLE_UPDATE_VIA_API:
            return switch_on
        return default

    test_application_name = test_application.application_name
    for endpoint, users in allow_users.items():
        if '{application}' in users:
            users.append(test_application_name)
    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    mocker.patch.object(settings, 'ALLOW_UPDATE_VIA_API_USERS', allow_users)
    mocker.patch.object(settings, 'AUTH_SPREAD_WHITELIST',
                        [test_application_name])

    build_zk_tree([('/test_cluster', '233')])
    url = path.format(**{'application': test_application.application_name})
    headers = {'Authorization': test_application_token}
    if is_fe:
        headers['X-Frontend-Name'] = 'arch.huskar_fe'
        headers['Authorization'] = test_token
        test_application.ensure_auth(Authority.WRITE, test_user.id)
    r = client.post(url, headers=headers, data=data)
    assert r.status_code == status_code
    assert r.json['message'] == message


@mark.parametrize('user', [True, False])
def test_dont_trace_user_request(
        mocker, client, test_application_token, monitor_client, test_token,
        test_application, user):
    if user:
        headers = {'Authorization': test_token}
    else:
        headers = {'Authorization': test_application_token}
    r = client.get('/api/application', headers=headers)
    assert r.status_code == 200
    trace_names = [
        call[0][0] for call in monitor_client.increment.call_args_list]
    if user:
        assert 'access_via_api.all' not in trace_names
    else:
        assert 'access_via_api.all' in trace_names


def test_update_allow_all_via_api_endpoints_empty():
    default = deepcopy(settings.must_allow_all_via_api_endpoints)
    assert settings.ALLOW_ALL_VIA_API_ENDPOINTS == default
    try:
        settings.update_allow_all_via_api_endpoints({})
        assert settings.ALLOW_ALL_VIA_API_ENDPOINTS == default
    finally:
        settings.ALLOW_ALL_VIA_API_ENDPOINTS = default


def test_update_allow_all_via_api_endpoints_only_fetch():
    default = deepcopy(settings.must_allow_all_via_api_endpoints)
    assert settings.ALLOW_ALL_VIA_API_ENDPOINTS == default

    expected = deepcopy(default)
    input = ['api.test', 'api.service']
    fetch_result = list(expected['fetch'])
    fetch_result.extend(input)
    expected['fetch'] = frozenset(fetch_result)
    try:
        settings.update_allow_all_via_api_endpoints({
            'fetch': input,
        })
        assert settings.ALLOW_ALL_VIA_API_ENDPOINTS == expected
    finally:
        settings.ALLOW_ALL_VIA_API_ENDPOINTS = default


def test_update_allow_all_via_api_endpoints_only_update():
    default = deepcopy(settings.must_allow_all_via_api_endpoints)
    assert settings.ALLOW_ALL_VIA_API_ENDPOINTS == default

    expected = deepcopy(default)
    input = ['api.test', 'api.service']
    update_result = list(expected['update'])
    update_result.extend(input)
    expected['update'] = frozenset(update_result)
    try:
        settings.update_allow_all_via_api_endpoints({
            'update': input,
        })
        assert settings.ALLOW_ALL_VIA_API_ENDPOINTS == expected
    finally:
        settings.ALLOW_ALL_VIA_API_ENDPOINTS = default


def test_update_allow_all_via_api_endpoints_fetch_and_update():
    default = deepcopy(settings.must_allow_all_via_api_endpoints)
    assert settings.ALLOW_ALL_VIA_API_ENDPOINTS == default

    expected = deepcopy(default)
    fetch_input = ['api.test', 'api.service']
    update_input = ['api.hello', 'api.config']
    fetch_result = list(expected['fetch'])
    fetch_result.extend(fetch_input)
    expected['fetch'] = frozenset(fetch_result)
    update_result = list(expected['update'])
    update_result.extend(update_input)
    expected['update'] = frozenset(update_result)
    try:
        settings.update_allow_all_via_api_endpoints({
            'fetch': fetch_input,
            'update': update_input,
        })
        assert settings.ALLOW_ALL_VIA_API_ENDPOINTS == expected
    finally:
        settings.ALLOW_ALL_VIA_API_ENDPOINTS = default


def test_update_allow_fetch_via_api_users():
    assert settings.ALLOW_FETCH_VIA_API_USERS == {}
    expected = {
        'api.switch': ['*'],
        'api.test': ['foo.test', 'lisi', 'wang.wu'],
    }
    try:
        settings.update_allow_fetch_via_api_users(expected)
        assert settings.ALLOW_FETCH_VIA_API_USERS == expected
    finally:
        settings.ALLOW_FETCH_VIA_API_USERS = {}


def test_update_allow_update_via_api_users():
    assert settings.ALLOW_UPDATE_VIA_API_USERS == {}
    expected = {
        'api.service': ['*'],
        'api.config': ['foo.test', 'lisi'],
    }
    try:
        settings.update_allow_update_via_api_users(expected)
        assert settings.ALLOW_UPDATE_VIA_API_USERS == expected
    finally:
        settings.ALLOW_UPDATE_VIA_API_USERS = {}
