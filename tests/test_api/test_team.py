from __future__ import absolute_import

from pytest import fixture

from huskar_api.models.auth import Team, TeamAdmin
from ..utils import assert_response_ok


@fixture(scope='function')
def test_team_admin(db, test_team, test_user):
    test_team.grant_admin(test_user.id)
    return test_user


def test_add_team(client, db, admin_token):
    assert db.query(Team.team_name).all() == []

    r = client.post('/api/team', data={'team': 'foo'},
                    headers={'Authorization': admin_token})
    assert r.status_code == 201
    assert r.json['status'] == 'SUCCESS'

    assert db.query(Team.team_name).all() == [('foo',)]


def test_add_team_without_authority(client, db, test_token):
    assert db.query(Team.team_name).all() == []

    r = client.post('/api/team', data={'team': 'foo'},
                    headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'

    assert db.query(Team.team_name).all() == []


def test_add_duplicate_team(client, db, admin_token, test_team):
    assert db.query(Team.id).all() == [(test_team.id,)]

    r = client.post('/api/team', data={'team': test_team.team_name},
                    headers={'Authorization': admin_token})
    assert_response_ok(r)

    assert db.query(Team.id).all() == [(test_team.id,)]


def test_recreate_archived_team(client, db, admin_token, test_team):
    test_team.archive()
    team_name = test_team.team_name
    r = client.post('/api/team', data={'team': team_name},
                    headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['message'] == (u'Team %s has been archived.' % team_name)


def test_delete_team(client, db, admin_token, test_team, test_team_admin):
    assert db.query(Team.id).all() == [(test_team.id,)]
    assert [x.id for x in test_team.list_admin()] == [test_team_admin.id]

    r = client.delete('/api/team/%s' % test_team.team_name,
                      headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert [x.id for x in test_team.list_admin()] == \
        [test_team_admin.id]
    assert Team.get(test_team.id).is_active is False


def test_delete_team_failed(client, db, admin_token, test_team,
                            test_application):
    assert db.query(Team.id).all() == [(test_team.id,)]

    r = client.delete('/api/team/%s' % test_team.team_name,
                      headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'TeamNotEmptyError'


def test_delete_not_exist_team(client, db, admin_token):
    assert db.query(Team.team_name).all() == []

    r = client.delete('/api/team/foo', headers={'Authorization': admin_token})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'

    assert db.query(Team.team_name).all() == []


def test_get_team_list(client, test_token, test_team, minimal_mode):
    if minimal_mode:
        expected_name = 'default'
    else:
        expected_name = test_team.team_name

    r = client.get('/api/team', headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.json['data']['teams'] == [
        {'name': expected_name, 'desc': expected_name}]


def test_add_team_admin(client, db, admin_token, test_team, test_user,
                        last_audit_log, mocker):
    assert db.query(TeamAdmin.team_id, TeamAdmin.user_id).all() == []

    r = client.post('/api/auth/team/%s+1s' % test_team.team_name,
                    data={'username': test_user.username},
                    headers={'Authorization': admin_token})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert db.query(TeamAdmin.team_id, TeamAdmin.user_id).all() == []
    assert last_audit_log() is None

    db.close()

    r = client.post('/api/auth/team/%s' % test_team.team_name,
                    data={'username': test_user.username},
                    headers={'Authorization': admin_token})
    assert r.status_code == 201
    assert r.json['status'] == 'SUCCESS'
    assert db.query(TeamAdmin.team_id, TeamAdmin.user_id).all() == [
        (test_team.id, test_user.id)]
    audit_log = last_audit_log()
    assert audit_log.action_name == 'GRANT_TEAM_ADMIN'
    assert audit_log.action_json['team_name'] == test_team.team_name
    assert audit_log.action_json['team_desc'] == test_team.team_desc
    assert audit_log.action_json['username'] == test_user.username


def test_add_team_admin_without_authority(
        client, db, test_token, test_team, test_user, last_audit_log):
    assert [x.id for x in test_team.list_admin()] == []

    r = client.post('/api/auth/team/%s' % test_team.team_name,
                    data={'username': test_user.username},
                    headers={'Authorization': test_token})
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'

    assert [x.id for x in test_team.list_admin()] == []
    assert last_audit_log() is None


def test_add_duplicate_team_admin(
        client, db, admin_token, test_team, test_user, test_team_admin,
        last_audit_log):
    assert [x.id for x in test_team.list_admin()] == [test_team_admin.id]

    r = client.post('/api/auth/team/%s' % test_team.team_name,
                    data={'username': test_user.username},
                    headers={'Authorization': admin_token})
    assert_response_ok(r)

    assert [x.id for x in test_team.list_admin()] == [test_team_admin.id]
    assert last_audit_log() is None


def test_add_team_admin_not_found(client, db, faker, admin_token, test_team,
                                  last_audit_log):
    username = faker.uuid4()
    r = client.post('/api/auth/team/%s' % test_team.team_name,
                    data={'username': username},
                    headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'user %s does not exist' % username
    assert last_audit_log() is None


def test_delete_team_admin(
        client, db, admin_token, test_team, test_user, test_team_admin,
        last_audit_log):
    assert [x.id for x in test_team.list_admin()] == [test_team_admin.id]

    r = client.delete('/api/auth/team/%s+1s' % test_team.team_name,
                      data={'username': test_team_admin.username},
                      headers={'Authorization': admin_token})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert [x.id for x in test_team.list_admin()] == [test_team_admin.id]

    r = client.delete('/api/auth/team/%s' % test_team.team_name,
                      data={'username': test_team_admin.username},
                      headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert [x.id for x in test_team.list_admin()] == []
    audit_log = last_audit_log()
    assert audit_log.action_name == 'DISMISS_TEAM_ADMIN'
    assert audit_log.action_json['team_name'] == test_team.team_name
    assert audit_log.action_json['team_desc'] == test_team.team_desc
    assert audit_log.action_json['username'] == test_user.username


def test_delete_not_exist_team_admin(
        client, db, admin_token, test_team, test_user, last_audit_log):
    assert db.query(TeamAdmin.id).all() == []

    r = client.delete('/api/auth/team/%s' % test_team.team_name,
                      data={'username': test_user.username},
                      headers={'Authorization': admin_token})
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'

    assert [x.id for x in test_team.list_admin()] == []
    assert last_audit_log() is None


def test_list_team_admin(
        client, db, admin_token, test_team, test_user, test_team_admin):
    r = client.get('/api/auth/team/%s' % test_team.team_name,
                   headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert r.json['data'] == {'admin': [test_user.username]}

    r = client.get('/api/auth/team/%s+1s' % test_team.team_name,
                   headers={'Authorization': admin_token})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
