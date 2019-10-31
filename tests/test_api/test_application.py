from __future__ import absolute_import

import json

from gevent import sleep
from pytest import fixture, mark

from huskar_api import settings
from huskar_api.models.auth import (
    User, Team, Application, ApplicationAuth, Authority)
from huskar_api.switch import switch, SWITCH_VALIDATE_SCHEMA
from ..utils import assert_response_ok


@fixture
def fake_team(db, faker):
    return Team.create(faker.uuid4()[:8], faker.uuid4()[:8])


def _setup_apps(present_apps, present_auth, team_id, user_id):
    apps = {
        name: Application.create(name, team_id) for name in present_apps}
    for auth in present_auth:
        authority = Authority(auth['auth'])
        apps[auth['name']].ensure_auth(authority, user_id)
    return apps


def _list_auth(db):
    return sorted([{
        'auth': auth.authority,
        'name': Application.get(auth.application_id).application_name,
        'user': User.get(auth.user_id).username,
    } for auth in db.query(ApplicationAuth)])


def _format_auth(auth_list, **kwargs):
    return sorted([{
        k: v.format(**kwargs) for k, v in auth.items()
    } for auth in auth_list])


@mark.xparametrize
def test_list_applications_under_team(
        client, zk, test_token, fake_team, minimal_mode, present, expected):
    for name in present:
        if minimal_mode:
            zk.ensure_path('/huskar/config/%s' % name)
            sleep(0.1)
        else:
            Application.create(name, fake_team.id)

    if minimal_mode:
        r = client.get('/api/team/%s+1s' % Team.DEFAULT_NAME,
                       headers={'Authorization': test_token})
        assert r.status_code == 404
        assert r.json['status'] == 'NotFound'

        r = client.get('/api/team/%s' % Team.DEFAULT_NAME,
                       headers={'Authorization': test_token})
        assert_response_ok(r)
        assert set(expected).issubset(r.json['data'])
    else:
        r = client.get('/api/team/%s+1s' % fake_team.team_name,
                       headers={'Authorization': test_token})
        assert r.status_code == 404
        assert r.json['status'] == 'NotFound'

        r = client.get('/api/team/%s' % fake_team.team_name,
                       headers={'Authorization': test_token})
        assert_response_ok(r)
        assert r.json['data'] == expected


@mark.xparametrize
def test_list_all_applications(
        client, test_user, test_token, admin_token, fake_team,
        zk, minimal_mode, mocker,
        present_apps, present_auth, expected_all, expected_authorized):
    if minimal_mode:
        for app in present_apps:
            zk.ensure_path('/huskar/service/%s' % app)
    else:
        _setup_apps(present_apps, present_auth, fake_team.id, test_user.id)

    r = client.get('/api/application', headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.headers.get('ETag')
    assert 'Cache-Control' not in r.headers
    if minimal_mode:
        assert set(expected_all).issubset(d['name'] for d in r.json['data'])
        assert all(d['team_name'] == 'default' for d in r.json['data'])
        assert all(d['team_desc'] == 'default' for d in r.json['data'])
    else:
        assert set(d['name'] for d in r.json['data']) == set(expected_all)
        assert all(d['team_name'] == fake_team.team_name
                   for d in r.json['data'])
        assert all(d['team_desc'] == fake_team.team_desc
                   for d in r.json['data'])

    mocker.patch.object(settings, 'CACHE_CONTROL_SETTINGS', {
        'api.application': {'max_age': 233},
    })
    r = client.get('/api/application', headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.headers.get('ETag')
    if minimal_mode:
        assert 'Cache-Control' not in r.headers
    else:
        assert r.headers.get('Cache-Control') == 'max-age=233'

    r = client.get('/api/application', query_string={'with_authority': '1'},
                   headers={'Authorization': test_token})
    assert_response_ok(r)
    if minimal_mode:
        return
    assert set(d['name'] for d in r.json['data']) == set(expected_authorized)
    assert all(d['team_name'] == fake_team.team_name for d in r.json['data'])
    assert all(d['team_desc'] == fake_team.team_desc for d in r.json['data'])


def test_get_application(
        client, test_user, test_token, fake_team, zk, minimal_mode, mocker):
    # Ensure is_deprecated and is_blacklisted will be stability
    zk.delete('/huskar/service/arch.huskar_api/config', recursive=True)

    if minimal_mode:
        zk.ensure_path('/huskar/service/base.foo')
        zk.delete('/huskar/service/base.bar', recursive=True)
        zk.delete('/huskar/switch/base.bar', recursive=True)
        zk.delete('/huskar/config/base.bar', recursive=True)
    else:
        _setup_apps(['base.foo'], [], fake_team.id, test_user.id)

    r = client.get('/api/application/base.foo', headers={
        'Authorization': test_token,
    })
    assert_response_ok(r)
    assert r.json['data']['item']['name'] == 'base.foo'
    if minimal_mode:
        assert r.json['data']['item']['team_name'] == 'default'
        assert r.json['data']['item']['team_desc'] == 'default'
    else:
        assert r.json['data']['item']['team_name'] == fake_team.team_name
        assert r.json['data']['item']['team_desc'] == fake_team.team_desc
    assert r.json['data']['item']['is_deprecated'] is False
    assert r.json['data']['item']['is_blacklisted'] is False
    assert isinstance(r.json['data']['item']['route_stage'], dict)

    r = client.get('/api/application/base.bar', headers={
        'Authorization': test_token,
    })
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'application does not exist'
    assert r.json['data'] is None


@mark.xparametrize
def test_list_applications_with_route_stage(
        client, test_user, test_token, fake_team, zk,
        present_apps, present_route_stage, expected_all):
    _setup_apps(present_apps, [], fake_team.id, test_user.id)
    zk.delete('/huskar/config/arch.huskar_api', recursive=True)
    zk.create('/huskar/config/arch.huskar_api/alta1-test-1', makepath=True)
    zk.create('/huskar/config/arch.huskar_api/altb1-test-1', makepath=True)
    for cluster_name, route_stage_table in present_route_stage.items():
        path = '/huskar/config/arch.huskar_api/{0}/ROUTE_HIJACK_LIST'.format(
            cluster_name)
        zk.create(path, json.dumps(route_stage_table))
    r = client.get('/api/application', headers={'Authorization': test_token})
    assert_response_ok(r)
    returned_all = [
        {'name': item['name'], 'route_stage': item['route_stage']}
        for item in r.json['data']]
    assert sorted(returned_all) == sorted(expected_all)


@mark.xparametrize
def test_list_deprecated_applications(
        mocker, client, test_user, test_token, admin_token, fake_team,
        zk, minimal_mode, present_apps, blacklist, legacylist, expected_apps):
    if minimal_mode:
        for app in present_apps:
            zk.ensure_path('/huskar/service/%s' % app)
    else:
        _setup_apps(present_apps, [], fake_team.id, test_user.id)

    mocker.patch.object(
        settings, 'AUTH_APPLICATION_BLACKLIST', frozenset(blacklist))
    mocker.patch.object(
        settings, 'LEGACY_APPLICATION_LIST', frozenset(legacylist))

    r = client.get('/api/application', headers={'Authorization': test_token})
    assert_response_ok(r)
    for d in r.json['data']:
        if minimal_mode and d['name'] not in present_apps:
            continue
        assert d['is_deprecated'] == expected_apps[d['name']]


@mark.xparametrize
def test_add_application(
        client, db, last_audit_log, test_user, admin_token, fake_team,
        present_apps, present_auth, adding_app, expected_apps, expected_auth,
        webhook_backends):
    _setup_apps(present_apps, present_auth, fake_team.id, test_user.id)

    data = {'team': fake_team.team_name, 'application': adding_app}
    headers = {'Authorization': admin_token}
    r = client.post('/api/application', data=data, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] is None

    rows = db.query(Application.application_name).all()
    assert set([row[0] for row in rows]) == set(expected_apps)
    assert _list_auth(db) == _format_auth(expected_auth, test_user=test_user)

    audit_log = last_audit_log()
    assert audit_log.action_name == 'CREATE_APPLICATION'
    assert audit_log.action_json['application_name'] == data['application']
    assert audit_log.action_json['team_name'] == data['team']
    assert audit_log.action_json['team_desc'] == fake_team.team_desc

    for result in webhook_backends:
        assert result['action_name'] == 'CREATE_APPLICATION'
        assert result['action_data']['application_name'] == adding_app
        assert result['action_data']['team_name'] == fake_team.team_name
        assert result['action_data']['team_desc'] == fake_team.team_desc


@mark.parametrize('present_apps,adding_app', [
    (['foo', 'bar'], ['bar']),
])
def test_add_application_with_put_method(
        client, test_user, admin_token, fake_team, present_apps, adding_app):
    present_auth = [{'name': app, 'auth': 'write'} for app in present_apps]
    _setup_apps(present_apps, present_auth, fake_team.id, test_user.id)

    data = {'team': fake_team.team_name, 'application': adding_app}
    headers = {'Authorization': admin_token}
    r = client.put('/api/application', data=data, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] is None


def test_add_application_without_validation(
        client, mocker, test_user, admin_token, fake_team):
    invalid_name = '@_@::+1s'
    data = {'team': fake_team.team_name, 'application': invalid_name}
    headers = {'Authorization': admin_token}

    r = client.post('/api/application', data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'ValidationError'
    assert json.loads(r.json['message'])['name'][0]
    assert Application.get_by_name(invalid_name) is None

    def fake_switch(name, default=True):
        if name == SWITCH_VALIDATE_SCHEMA:
            return False
        return default
    mocker.patch.object(switch, 'is_switched_on', fake_switch)

    r = client.post('/api/application', data=data, headers=headers)
    assert_response_ok(r)
    assert Application.get_by_name(invalid_name)


@mark.xparametrize
def test_add_application_without_permission(
        client, db, last_audit_log, test_user, test_token, fake_team,
        present_apps, present_auth, adding_app):
    _setup_apps(present_apps, present_auth, fake_team.id, test_user.id)

    data = {'team': fake_team.team_name, 'application': adding_app}
    headers = {'Authorization': test_token}
    r = client.post('/api/application', data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert r.json['data'] is None

    # nothing changed
    rows = db.query(Application.application_name).all()
    assert set([row[0] for row in rows]) == set(present_apps)
    assert _list_auth(db) == _format_auth(present_auth + [
        {'name': name, 'user': name, 'auth': 'write'}
        for name in present_apps
    ], test_user=test_user)

    assert last_audit_log() is None


def test_add_application_twice(client, db, test_user, admin_token, fake_team):
    _setup_apps(['foo'], [], fake_team.id, test_user.id)

    data = {'team': fake_team.team_name, 'application': 'foo'}
    headers = {'Authorization': admin_token}
    r = client.post('/api/application', data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'ApplicationExistedError'
    assert r.json['data'] is None


def test_add_application_with_username(
        client, test_user, admin_token, fake_team):
    _setup_apps(['foo'], [], fake_team.id, test_user.id)

    data = {'team': fake_team.team_name, 'application': test_user.username}
    headers = {'Authorization': admin_token}
    r = client.post('/api/application', data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == (
        'The application name {0} has been occupied.'.format(
            test_user.username))
    assert r.json['data'] is None


def test_add_application_under_unknown_team(client, faker, db, admin_token):
    data = {'team': faker.uuid4()[:32], 'application': 'foo'}
    headers = {'Authorization': admin_token}
    r = client.post('/api/application', data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'team "%(team)s" does not exist' % data
    assert r.json['data'] is None


def test_recreate_archived_application(
        client, faker, db, admin_token, fake_team, test_user):
    apps = _setup_apps(['foo'], [], fake_team.id, test_user.id)
    app = apps['foo']
    app.archive()
    application_name = app.application_name

    data = {'team': fake_team.team_name, 'application': application_name}
    headers = {'Authorization': admin_token}
    r = client.post('/api/application', data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['message'] == (
        'The application name {0} has been occupied.'.format(application_name))


@mark.xparametrize
def test_add_application_with_invalid_name(
        client, fake_team, admin_token, adding_app, err_app):
    data = {'team': fake_team.team_name, 'application': adding_app}
    headers = {'Authorization': admin_token}
    r = client.post('/api/application', data=data, headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'ValidationError'
    assert r.json['message'] == (
        u'{"name": ["AppID(%s) should consist by most 128 '
        u'characters of numbers, lowercase letters '
        u'and underscore."]}' % err_app)


@mark.xparametrize
def test_delete_application(
        client, db, last_audit_log, test_user, test_token, admin_token,
        fake_team, present_apps, present_auth, deleting_app, expected_apps):
    _setup_apps(present_apps, present_auth, fake_team.id, test_user.id)
    prev_auths = _list_auth(db)

    r = client.delete('/api/application/%s' % deleting_app,
                      headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert r.json['data'] is None

    rows = db.query(Application.application_name) \
             .filter_by(status=Application.STATUS_ACTIVE).all()
    assert set([row[0] for row in rows]) == set(expected_apps)
    assert _list_auth(db) == prev_auths

    audit_log = last_audit_log()
    assert audit_log.action_name == 'ARCHIVE_APPLICATION'
    assert audit_log.action_json['application_name'] == deleting_app


@mark.xparametrize
def test_delete_application_without_permission(
        client, db, last_audit_log, test_user, test_token, fake_team,
        present_apps, present_auth, deleting_app):
    _setup_apps(present_apps, present_auth, fake_team.id, test_user.id)
    prev_auths = _list_auth(db)

    r = client.delete('/api/application/%s' % deleting_app,
                      headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert r.json['data'] is None

    # nothing changed
    rows = db.query(Application.application_name).all()
    assert set([row[0] for row in rows]) == set(present_apps)
    assert _list_auth(db) == prev_auths
    assert last_audit_log() is None


@mark.xparametrize
def test_delete_application_twice(client, db, faker, admin_token):
    r = client.delete('/api/application/%s' % faker.uuid4(),
                      headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'ApplicationNotExistedError'
    assert r.json['data'] is None
