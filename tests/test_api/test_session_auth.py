from __future__ import absolute_import

from flask import g
from gevent import sleep
from pytest import fixture, mark

from huskar_api import settings
from huskar_api.api.utils import minimal_mode_incompatible, api_response
from huskar_api.app import create_app
from huskar_api.models.auth import User
from huskar_api.switch import switch, SWITCH_ENABLE_MINIMAL_MODE
from ..utils import assert_response_ok


@fixture
def app():
    app = create_app()

    @app.route('/whoami')
    def whoami():
        if not g.auth:
            return '', 401
        return api_response(data=dict(
            username=g.auth.username,
            is_application=g.auth.is_application,
            is_admin=g.auth.is_admin))

    @app.route('/incompatible')
    @minimal_mode_incompatible
    def incompatible():
        return ''

    return app


def test_anonymous(client):
    r = client.get('/whoami')
    assert r.status_code == 401


def test_authenticated_user(client, test_user, test_token):
    r = client.get('/whoami', headers={'Authorization': test_token})
    assert_response_ok(r)
    data = r.json['data']
    assert data['username'] == test_user.username
    assert data['is_application'] is False
    assert data['is_admin'] is False


def test_authenticated_admin(client, admin_user, admin_token):
    r = client.get('/whoami', headers={'Authorization': admin_token})
    assert_response_ok(r)
    data = r.json['data']
    assert data['username'] == admin_user.username
    assert data['is_application'] is False
    assert data['is_admin'] is True


def test_authenticated_application(client, test_application,
                                   test_application_token):
    r = client.get('/whoami',
                   headers={'Authorization': test_application_token})
    assert_response_ok(r)
    data = r.json['data']
    assert data['username'] == test_application.application_name
    assert data['is_application'] is True
    assert data['is_admin'] is False


def test_authenticated_application_with_fallback_token(
        client, test_application, test_application_fallback_token,
        monitor_client):
    r = client.get(
        '/whoami', headers={'Authorization': test_application_fallback_token}
        )
    application_name = test_application.application_name
    assert_response_ok(r)
    data = r.json['data']
    assert data['username'] == application_name
    assert data['is_application'] is True
    assert data['is_admin'] is False
    monitor_client.increment.assert_any_call(
        'old_token', tags=dict(res=application_name))


def test_authenticated_user_in_minimal_mode(client, faker, mocker, broken_db):
    test_user = User(username=faker.safe_email().split('@')[0])
    test_token = test_user.generate_token(settings.SECRET_KEY)

    mocker.patch.object(settings, 'ADMIN_EMERGENCY_USER_LIST', ['admin'])

    r = client.get('/whoami', headers={'Authorization': test_token})
    assert_response_ok(r)
    data = r.json['data']
    assert data['username'] == test_user.username
    assert data['is_application'] is False
    assert data['is_admin'] is False


def test_authenticated_admin_in_minimal_mode(client, faker, mocker, broken_db):
    test_user = User(username=faker.safe_email().split('@')[0])
    test_token = test_user.generate_token(settings.SECRET_KEY)

    mocker.patch.object(settings, 'ADMIN_EMERGENCY_USER_LIST', [
        test_user.username,
    ])

    r = client.get('/whoami', headers={'Authorization': test_token})
    assert_response_ok(r)
    data = r.json['data']
    assert data['username'] == test_user.username
    assert data['is_application'] is False
    assert data['is_admin'] is True


@mark.parametrize('data_type', ['service', 'switch', 'config'])
def test_authenticated_application_in_minimal_mode(client, zk, faker, mocker,
                                                   broken_db, data_type):
    test_user = User(username=faker.uuid4()[:8])
    test_token = test_user.generate_token(settings.SECRET_KEY)

    zk_path = '/huskar/%s/%s' % (data_type, test_user.username)
    zk.ensure_path(zk_path)
    sleep(0.1)

    r = client.get('/whoami', headers={'Authorization': test_token})
    assert_response_ok(r)
    data = r.json['data']
    assert data['username'] == test_user.username
    assert data['is_application'] is True
    assert data['is_admin'] is False


def test_enable_minimal_mode_by_switch(client, faker, mocker):
    test_user = User(username=faker.safe_email().split('@')[0])
    test_token = test_user.generate_token(settings.SECRET_KEY)

    assert User.get_by_name(test_user.username) is None

    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_MINIMAL_MODE:
            return True
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    mocker.patch.object(settings, 'ADMIN_EMERGENCY_USER_LIST', [
        test_user.username,
    ])

    r = client.get('/whoami', headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.headers['X-Minimal-Mode'] == '1'
    assert r.headers['X-Minimal-Mode-Reason'] == 'switch'
    data = r.json['data']
    assert data['username'] == test_user.username
    assert data['is_application'] is False
    assert data['is_admin'] is True


def test_response_header(client, test_user, test_token):
    r = client.get('/whoami', headers={'Authorization': test_token})
    assert_response_ok(r)
    assert 'X-Minimal-Mode' not in r.headers
    assert 'X-Minimal-Mode-Reason' not in r.headers


def test_response_header_in_minimal_mode(client, faker, broken_db):
    test_user = User(username=faker.safe_email().split('@')[0])
    test_token = test_user.generate_token(settings.SECRET_KEY)

    r = client.get('/whoami', headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.headers['X-Minimal-Mode'] == '1'
    assert r.headers['X-Minimal-Mode-Reason'] == 'auth'


def test_minimal_mode_incompatible(client, faker, broken_db):
    test_user = User(username=faker.safe_email().split('@')[0])
    test_token = test_user.generate_token(settings.SECRET_KEY)

    r = client.get('/incompatible', headers={'Authorization': test_token})
    assert r.status_code == 501
    assert r.json['status'] == 'NotImplemented'
    assert r.json['message'] == ('Current API is not suitable for working '
                                 'in minimal mode')
    assert r.json['data'] is None


def test_doctor_in_minimal_mode(app, client, mocker, faker, broken_db):
    metrics = app.extensions['huskar_api.db.tester'].metrics
    on_sys_exc = mocker.patch.object(
        metrics, 'on_api_called_sys_exc', autospec=True)
    on_ok = mocker.patch.object(metrics, 'on_api_called_ok', autospec=True)

    test_user = User(username=faker.safe_email().split('@')[0])
    test_token = test_user.generate_token(settings.SECRET_KEY)

    client.get('/whoami', headers={'Authorization': test_token})

    assert not on_ok.called
    assert on_sys_exc.called


def test_blacklisted_application_token(
        client, test_application, test_application_token, mocker,
        minimal_mode):
    application_name = test_application.application_name
    settings.update_application_blacklist([application_name])
    r = client.get(
        '/whoami', headers={'Authorization': test_application_token}
        )
    assert r.status_code == 403
    assert r.json['message'] == 'application: {} is blacklisted'.format(
        application_name)
    settings.update_application_blacklist([])
