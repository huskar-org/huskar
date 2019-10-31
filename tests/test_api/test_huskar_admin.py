from __future__ import absolute_import

from huskar_api.models.auth import User
from ..utils import assert_response_ok


def test_add_huskar_admin(client, db, last_audit_log, admin_token, test_user):
    r = client.post('/api/auth/huskar', data={'username': test_user.username},
                    headers={'Authorization': admin_token})
    assert r.status_code == 201
    assert r.json['status'] == 'SUCCESS'
    assert db.query(User.huskar_admin).filter_by(
        username=test_user.username).scalar() == 1
    audit_log = last_audit_log()
    assert audit_log.action_name == 'GRANT_HUSKAR_ADMIN'
    assert audit_log.action_json['username'] == test_user.username


def test_add_huskar_admin_twice(
        client, db, last_audit_log, admin_token, test_user):
    test_user.grant_admin()

    r = client.post('/api/auth/huskar', data={'username': test_user.username},
                    headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert last_audit_log() is None


def test_add_huskar_admin_not_found(
        client, db, faker, last_audit_log, admin_token):
    username = faker.uuid4()
    r = client.post('/api/auth/huskar', data={'username': username},
                    headers={'Authorization': admin_token})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'user "%s" is not found' % username
    assert last_audit_log() is None


def test_add_huskar_admin_without_authority(
        client, last_audit_log, test_token, test_user):
    r = client.post('/api/auth/huskar', data={'username': test_user.username},
                    headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert last_audit_log() is None


def test_delete_huskar_admin(
        client, db, last_audit_log, admin_token, test_user):
    test_user.grant_admin()

    r = client.delete('/api/auth/huskar/%s' % test_user.username,
                      headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert db.query(User.huskar_admin).filter_by(
        username=test_user.username).scalar() == 0
    audit_log = last_audit_log()
    assert audit_log.action_name == 'DISMISS_HUSKAR_ADMIN'
    assert audit_log.action_json['username'] == test_user.username


def test_delete_huskar_admin_himself(
        client, db, last_audit_log, admin_user, admin_token):
    r = client.delete('/api/auth/huskar/%s' % admin_user.username,
                      headers={'Authorization': admin_token})
    assert r.status_code == 403
    assert r.json['status'] == 'Forbidden'
    assert db.query(User.huskar_admin).filter_by(
        username=admin_user.username).scalar() == 1
    assert last_audit_log() is None


def test_delete_huskar_admin_without_authority(
        client, last_audit_log, test_token, test_user, admin_user):
    r = client.delete('/api/auth/huskar/%s' % admin_user.username,
                      headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert last_audit_log() is None
