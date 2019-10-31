from __future__ import absolute_import

from pytest import fixture, mark

from huskar_api.models.auth import User, ApplicationAuth, Authority
from ..utils import assert_response_ok


@fixture
def add_user(faker):
    def factory(names):
        for name in names:
            if isinstance(name, list):
                name, email = name
            else:
                email = '%s@example.com' % name
            User.create_normal(
                name, faker.password(), email=email,
                is_active=True)
    return factory


@fixture
def add_application_auth(db, test_application, test_application_token):
    def factory(names):
        for name in names:
            username, authority = name.split(':', 1)
            user_id = db.query(User.id).filter_by(username=username).scalar()
            authority = Authority(authority)
            test_application.ensure_auth(authority, user_id)
    return factory


@fixture
def list_application_auth(db, test_application):
    def generator():
        for auth in db.query(ApplicationAuth).filter_by(
                application_id=test_application.id).all():
            user = db.query(User).get(auth.user_id)
            if not user.is_application:
                yield '%s:%s' % (user.username, auth.authority)
    return generator


@fixture
def format_values(test_application):
    def factory(d):
        template_vars = {'test_application': test_application.application_name}
        r = dict(d)
        r.update((k, v % template_vars) for k, v in d.items()
                 if isinstance(v, basestring))
        return r
    return factory


@mark.xparametrize
def test_add_application_auth(
        last_audit_log, add_user, add_application_auth, list_application_auth,
        present_user, present_auth, request_auth, expected_resp, expected_auth,
        client, test_application, admin_token):
    add_user(present_user)
    add_application_auth(present_auth)

    username, authority = request_auth.split(':', 1)
    r = client.post(
        '/api/auth/application/%s' % test_application.application_name,
        data={'username': username, 'authority': authority},
        headers={'Authorization': admin_token})
    assert r.status_code == expected_resp['status_code']
    assert r.json == expected_resp['content']

    assert set(list_application_auth()) == set(expected_auth)

    audit_log = last_audit_log()
    if expected_resp['status_code'] == 200:
        assert audit_log.action_name == 'GRANT_APPLICATION_AUTH'
        assert audit_log.action_json['application_name'] == \
            test_application.application_name
        assert audit_log.action_json['username'] == username
        assert audit_log.action_json['authority'] == authority
    else:
        assert audit_log is None


def test_add_application_auth_to_invalid_application(
        db, client, faker, add_user, admin_token, last_audit_log,
        test_application):
    add_user(['foo'])
    name = faker.uuid4()
    test_application.archive()
    r = client.post(
        '/api/auth/application/%s' % name,
        data={'username': 'foo', 'authority': 'read'},
        headers={'Authorization': admin_token})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'application %s does not exist' % name

    r = client.post(
        '/api/auth/application/%s' % test_application.application_name,
        data={'username': 'foo', 'authority': 'read'},
        headers={'Authorization': admin_token})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == ('application %s does not exist' %
                                 test_application.application_name)

    assert last_audit_log() is None


def test_add_application_auth_to_invalid_user(
        client, faker, add_user, admin_token, last_audit_log,
        test_application):
    add_user(['foo'])
    user = User.get_by_name('foo')
    user.archive()
    application_name = test_application.application_name

    unknow_user = faker.uuid4()[:6]
    r = client.post(
        '/api/auth/application/%s' % application_name,
        data={'username': unknow_user, 'authority': 'read'},
        headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'user %s does not exist' % unknow_user

    r = client.post(
        '/api/auth/application/%s' % application_name,
        data={'username': 'foo', 'authority': 'read'},
        headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'user foo does not exist'

    assert last_audit_log() is None


@mark.xparametrize
def test_delete_application_auth(
        add_user, add_application_auth, list_application_auth, format_values,
        present_user, present_auth, request_auth, expected_resp, expected_auth,
        client, test_application, admin_token, last_audit_log):
    add_user(present_user)
    add_application_auth(present_auth)

    username, authority = request_auth.split(':', 1)
    r = client.delete(
        '/api/auth/application/%s' % test_application.application_name,
        data={'username': username, 'authority': authority},
        headers={'Authorization': admin_token})
    assert r.status_code == expected_resp['status_code']
    assert r.json == format_values(expected_resp['content'])

    assert set(list_application_auth()) == set(expected_auth)

    audit_log = last_audit_log()
    if expected_resp['status_code'] == 200:
        assert audit_log.action_name == 'DISMISS_APPLICATION_AUTH'
        assert audit_log.action_json['application_name'] == \
            test_application.application_name
        assert audit_log.action_json['username'] == username
        assert audit_log.action_json['authority'] == authority
    else:
        assert audit_log is None


@mark.xparametrize
def test_list_application_auth(
        add_user, add_application_auth, list_application_auth, format_values,
        present_user, present_auth, expected_data,
        client, test_application, admin_token):
    add_user(present_user)
    add_application_auth(present_auth)

    r = client.get(
        '/api/auth/application/%s' % test_application.application_name,
        headers={'Authorization': admin_token})
    assert_response_ok(r)

    for item, expected_item in zip(
            reversed(r.json['data']['application_auth']),  # order by key desc
            expected_data['application_auth']):
        ex = format_values(expected_item)
        assert item['authority'] == ex['authority']
        assert item['user']['username'] == ex['username']
        assert item['user']['is_application'] == ex['is_application']
        assert item['user']['is_active'] is True
        assert item['user']['is_admin'] is False


def test_add_application_without_permission(
        client, test_user, test_token, test_application,
        list_application_auth, last_audit_log):
    r = client.post(
        '/api/auth/application/%s' % test_application.application_name,
        data={'username': test_user.username, 'authority': 'read'},
        headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert r.json['data'] is None
    assert set(list_application_auth()) == set([])
    assert last_audit_log() is None


@mark.parametrize('test_authority', ['unknow'])
def test_add_application_with_unknown_authority(
        client, test_user, test_application, test_authority, admin_token,
        list_application_auth, last_audit_log):
    r = client.post(
        '/api/auth/application/%s' % test_application.application_name,
        data={'username': test_user.username, 'authority': test_authority},
        headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['data'] is None
    assert set(list_application_auth()) == set([])
    assert last_audit_log() is None


@mark.parametrize('test_authority', ['unknow'])
def test_delete_application_with_unknown_authority(
        client, test_user, test_application, test_authority, admin_token):
    r = client.delete(
        '/api/auth/application/%s' % test_application.application_name,
        data={'username': test_user.username, 'authority': test_authority},
        headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['data'] is None
