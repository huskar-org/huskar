from __future__ import absolute_import

from flask import g
from freezegun import freeze_time
from pytest import fixture, mark

from huskar_api import settings
from huskar_api.models import DBSession
from huskar_api.models.auth import User, Application, Authority
from ..utils import assert_response_ok


@fixture
def fixture_token(request):
    assert request.param in ('test_application_token', 'test_token')
    return request.getfixturevalue(request.param)


def test_admin_user(admin_user):
    '''
    check the existence of admin user
    '''
    assert DBSession().query(User) \
                      .filter_by(username=admin_user.username) \
                      .first()


def test_get_user_token(client, mocker, admin_user):
    g.auth = None
    r = client.post('/api/auth/token', data={
        'username': admin_user.username, 'password': 'admin'})
    assert_response_ok(r)
    assert r.json['data']['expires_in'] == settings.ADMIN_MAX_EXPIRATION
    user = User.get_by_token(settings.SECRET_KEY, r.json['data']['token'])
    assert user and user.username == admin_user.username
    assert g.auth and g.auth.id == admin_user.id


@mark.parametrize('password', ['wrong', ''])
def test_get_user_token_with_wrong_password(client, admin_user, password):
    r = client.post('/api/auth/token', data={
        'username': admin_user.username, 'password': password})
    assert r.status_code == 400
    assert r.json['status'] == 'LoginError'
    assert r.json['data'] is None


def test_get_user_token_of_unknown_user(client):
    r = client.post('/api/auth/token', data={
        'username': 'lord.voldemort', 'password': 'you-know-who'})
    assert r.status_code == 400
    assert r.json['status'] == 'UserNotExistedError'
    assert r.json['data'] is None


def test_get_user_token_with_app_user(
        client, test_application, test_application_token):
    r = client.post('/api/auth/token', data={
        'username': test_application.application_name,
        'password': test_application.application_name})
    assert r.status_code == 400
    assert r.json['status'] == 'UserNotExistedError'
    assert r.json['data'] is None


def test_user_token_authorization(client, admin_user):
    r = client.post('/api/auth/token', data={
        'username': admin_user.username, 'password': 'admin'})
    assert_response_ok(r)

    token = r.json['data']['token']
    r = client.get('/api/user/admin', headers={'Authorization': token})
    assert_response_ok(r)


def test_user_token_expired(client, admin_user):
    with freeze_time('2012-12-12 00:00:00'):
        r = client.post('/api/auth/token', data={
            'username': admin_user.username, 'password': 'admin',
            'expiration': 30})
        assert_response_ok(r)
        token = r.json['data']['token']
        assert token

    with freeze_time('2012-12-12 00:01:00'):
        r = client.get('/api/user/admin', headers={'Authorization': token})
        assert r.status_code == 401
        assert r.json['status'] == 'Unauthorized'


@mark.parametrize('method', ['GET', 'POST'])
def test_get_application_token_by_writer(
        client, db, method, test_application, test_user, test_token):
    test_application.ensure_auth(Authority.WRITE, test_user.id)

    r = client.open(
        '/api/application/%s/token' % test_application.application_name,
        method=method, headers={'Authorization': test_token})
    assert_response_ok(r)
    user = User.get_by_token(settings.SECRET_KEY, r.json['data']['token'])
    assert user and user.username == test_application.application_name


def test_get_application_token_by_admin(
        client, db, test_application, admin_token):
    r = client.post(
        '/api/application/%s/token' % test_application.application_name,
        headers={'Authorization': admin_token})
    assert_response_ok(r)
    user = User.get_by_token(settings.SECRET_KEY, r.json['data']['token'])
    assert user and user.username == test_application.application_name


def test_get_application_token_without_permission(
        client, db, test_application, test_user, test_token):
    test_application.ensure_auth(Authority.READ, test_user.id)

    r = client.post(
        '/api/application/%s/token' % test_application.application_name,
        headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert r.json['data'] is None


def test_get_application_token_by_another_token(
        client, mocker, db, test_application, test_application_token):
    r = client.post(
        '/api/application/%s/token' % test_application.application_name,
        headers={'Authorization': test_application_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert r.json['data'] is None
    assert r.json['message'] == 'It is not permitted to exchange token'

    mocker.patch.object(settings, 'AUTH_SPREAD_WHITELIST', [
        test_application.application_name,
    ])
    r = client.post(
        '/api/application/%s/token' % test_application.application_name,
        headers={'Authorization': test_application_token})
    assert_response_ok(r)
    user = User.get_by_token(settings.SECRET_KEY, r.json['data']['token'])
    assert user and user.username == test_application.application_name


@mark.parametrize('check_application_auth', [True, False])
def test_get_application_token_with_unknown_application(
        client, db, test_application, admin_token,
        mocker, check_application_auth):
    if not check_application_auth:
        mocker.patch('huskar_api.api.organization.check_application_auth')

    r = client.post(
        '/api/application/%s+1s/token' % test_application.application_name,
        headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'ApplicationNotExistedError'
    assert r.json['data'] is None


def test_get_blacklisted_application_token(
        client, db, test_application, admin_token, mocker):
    application_name = test_application.application_name
    mocker.patch.object(settings, 'AUTH_APPLICATION_BLACKLIST',
                        frozenset([application_name]))

    r = client.post(
        '/api/application/%s/token' % application_name,
        headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'ApplicationNotExistedError'
    assert r.json['data'] is None


def test_get_application_token_with_orphan_application(
        client, faker, db, test_team, admin_token):
    _application = Application.create(faker.uuid4()[:8], test_team.id)
    _user = User.get_by_name(_application.application_name)
    _user.archive()

    user = User.get_by_name(_application.application_name)
    assert db.query(User).filter_by(
        username=_application.application_name, is_active=True).count() == 0

    r = client.post(
        '/api/application/%s/token' % _application.application_name,
        headers={'Authorization': admin_token})
    assert_response_ok(r)
    user = User.get_by_token(settings.SECRET_KEY, r.json['data']['token'])
    assert user and user.username == _application.application_name

    assert User.get_by_name(_application.application_name) is not None
    assert db.query(User).filter_by(
        username=_application.application_name).count() == 1


def test_get_application_token_with_malformed_application(
        client, faker, capsys, db, test_team, admin_token):
    _application = Application.create(faker.uuid4()[:8], test_team.id)
    _user = User.get_by_name(_application.application_name)
    with DBSession() as session:
        session.delete(_user)
        session.commit()
    _user = User.create_normal(
        _application.application_name, '-', is_active=True)

    r = client.post(
        '/api/application/%s/token' % _application.application_name,
        headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == (
        'malformed application: %s' % _application.application_name)
    assert r.json['data'] is None


def test_get_team_application_token_by_non_admin(
        client, db, test_team, test_application, test_user, test_token):
    r = client.post(
        '/api/team/%s/application/%s/token' % (
            test_team.team_name, test_application.application_name),
        data={'owner_email': test_user.username + '@a.b'},
        headers={'Authorization': test_token},
    )

    assert r.status_code == 403
    assert r.json['status'] == 'Forbidden'
    assert r.json['message'] == (
        'user "%s" has no admin permission' % test_user.username)


@mark.parametrize(
    'data,status',
    [(None, 'BadRequest'), ({'owner_email': 'hay'}, 'ValidationError'),
     ({'owner_email': 'hay@'}, 'ValidationError'),
     ({'owner_email': '@hay'}, 'ValidationError')])
def test_get_team_application_token_with_bad_request_data(
        client, db, test_team, test_application, admin_token, data, status):
    r = client.post(
        '/api/team/%s/application/%s/token' % (
            test_team.team_name, test_application.application_name),
        data=data,
        headers={'Authorization': admin_token},
    )

    assert r.status_code == 400
    assert r.json['status'] == status
    assert r.json['data'] is None


@mark.parametrize('application_name',
                  ['foo.you', 'foo.bar.what', 'foo.guess'])
def test_get_team_application_token_with_unknown_application(
        client, db, test_team, admin_user, admin_token, application_name):
    application = Application.get_by_name(application_name)
    assert application is None

    r = client.post(
        '/api/team/%s/application/%s/token' % (
            test_team.team_name, application_name),
        data={'owner_email': admin_user.username + '@foo.bar'},
        headers={'Authorization': admin_token},
    )

    assert_response_ok(r)
    application = Application.get_by_name(application_name)
    assert application is not None
    user = User.get_by_token(settings.SECRET_KEY, r.json['data']['token'])
    assert user and user.username == application.application_name


@mark.parametrize('owner_email', ['a.1@c.om', '2.b@c.om'])
def test_get_team_application_token_by_admin_with_unknown_owner_email(
        client, db, test_team, test_application, admin_user, admin_token,
        owner_email, mocker):
    r = client.post(
        '/api/team/%s/application/%s/token' % (
            test_team.team_name, test_application.application_name),
        data={'owner_email': owner_email},
        headers={'Authorization': admin_token}
    )

    assert_response_ok(r)
    user = User.get_by_token(settings.SECRET_KEY, r.json['data']['token'])
    assert user and user.username == test_application.application_name
    owner = owner_email.split('@', 1)[0]
    assert User.get_by_name(owner)


def test_get_team_application_token_with_malformed_application(
        client, faker, capsys, db, test_team, admin_user, admin_token):
    _application = Application.create(faker.uuid4()[:8], test_team.id)
    _user = User.get_by_name(_application.application_name)
    _user.is_app = False
    db.add(_user)
    db.commit()

    r = client.post(
        '/api/team/%s/application/%s/token' % (
            test_team.team_name, _application.application_name),
        data={'owner_email': admin_user.username + '@foo.bar'},
        headers={'Authorization': admin_token}
    )

    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == (
        'malformed application: %s' % _application.application_name)
    assert r.json['data'] is None


def test_get_team_application_token_with_malformed_user(
        client, db, test_team, test_application, admin_user, admin_token,
        faker):
    """The username and email prefix are not inconsistent."""
    User.create_normal(
        'foobar', faker.uuid4()[:8], 'foo.bar@example.com', True)
    r = client.post(
        '/api/team/%s/application/%s/token' % (
            test_team.team_name, test_application.application_name),
        data={'owner_email': 'foo.bar@example.com'},
        headers={'Authorization': admin_token}
    )
    assert_response_ok(r)


@mark.parametrize('fixture_token,f2e_name,status_code,status_text,message', [
    ('test_application_token', 'arch.huskar_fe', 403, 'Forbidden',
     'Using application token in web is not permitted.'),
    ('test_token', 'arch.huskar_fe', 200, 'SUCCESS', ''),
], indirect=['fixture_token'])
def test_application_token_abuse(
        client, mocker, fixture_token, f2e_name, status_code, status_text,
        message):
    headers = {'Authorization': fixture_token, 'X-Frontend-Name': f2e_name}
    r = client.get('/api/application', headers=headers)
    assert r.status_code == status_code
    assert r.json['status'] == status_text
    assert r.json['message'] == message
