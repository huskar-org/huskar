from __future__ import absolute_import

import json

from huskar_api.models.auth import Team
from huskar_api.models.audit import AuditLog
from huskar_api.api.organization import TeamView


def test_create_team(client, db, admin_user, admin_token, webhook_backends):
    assert Team.get_by_name('foo') is None
    assert db.query(AuditLog.id).first() is None

    r = client.post('/api/team', data={'team': 'foo'},
                    headers={'Authorization': admin_token})
    assert r.status_code == 201, r.json

    team = Team.get_by_name('foo')
    audit = db.query(AuditLog).first()
    assert team is not None
    assert audit is not None
    assert audit.user.id == admin_user.id
    assert audit.action_name == 'CREATE_TEAM'
    assert json.loads(audit.action_data) == {
        'team_id': team.id,
        'team_name': 'foo',
        'team_desc': team.team_desc,
    }
    assert audit.rollback_to is None

    for result in webhook_backends:
        assert result['action_data']['team_id'] == team.id
        assert result['action_data']['team_name'] == 'foo'
        assert result['action_name'] == 'CREATE_TEAM'


def test_create_team_failed(client, db, test_token):
    assert Team.get_by_name('foo') is None
    assert db.query(AuditLog.id).first() is None

    r = client.post('/api/team', data={'team': 'foo'},
                    headers={'Authorization': test_token})
    assert r.status_code == 400, r.json

    assert Team.get_by_name('foo') is None
    assert db.query(AuditLog.id).first() is None


def test_create_team_without_audit_log(client, mocker, db, admin_token):
    mocker.patch(
        'huskar_api.models.audit.AuditLog.create', autospec=True,
        spec_set=True, side_effect=RuntimeError)
    logger = mocker.patch('huskar_api.api.utils.logger', autospec=True)

    assert db.query(AuditLog.id).first() is None
    logger.exception.assert_not_called()

    r = client.post('/api/team', data={'team': 'foo'},
                    headers={'Authorization': admin_token})
    assert r.status_code == 201, r.json

    assert db.query(AuditLog.id).first() is None
    logger.exception.assert_called_once()


def test_create_team_while_audit_log_disabled(client, mocker, db, admin_token):
    def switch(name, default=True):
        # Switch off "enable_audit_log"
        if name in ('enable_audit_log', 'enable_minimal_mode'):
            return False
        return default
    mocker.patch('huskar_api.api.utils.switch.is_switched_on', switch)
    logger = mocker.patch('huskar_api.api.utils.logger', autospec=True)

    assert db.query(AuditLog.id).first() is None
    logger.exception.assert_not_called()

    r = client.post('/api/team', data={'team': 'foo'},
                    headers={'Authorization': admin_token})
    assert r.status_code == 201, r.json

    assert db.query(AuditLog.id).first() is None
    logger.exception.assert_not_called()


def test_audit_log_in_minimal_mode(
        client, mocker, admin_user, admin_token, broken_db):
    # Reverts the "minimal_mode_incompatible" decorator
    mocker.patch.object(TeamView, 'post', TeamView.post.original)

    mocker.patch('huskar_api.models.auth.Team.get_by_name', return_value=None)
    create = mocker.patch('huskar_api.models.auth.Team.create')
    create.return_value.id = 1
    create.return_value.team_name = 'foo'
    create.return_value.team_desc = 'foo_desc'

    logger = mocker.patch(
        'huskar_api.api.utils.fallback_audit_logger', autospec=True)
    logger.info.assert_not_called()

    r = client.post('/api/team', data={'team': 'foo'},
                    headers={'Authorization': admin_token})
    assert r.status_code == 201, r.json

    logger.info.assert_called_once_with(
        '%s %s %r', admin_user.username, 'CREATE_TEAM',
        {'team_id': 1, 'team_name': 'foo', 'team_desc': 'foo_desc'})
