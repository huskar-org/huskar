from __future__ import absolute_import

import datetime
import itertools
import json

from pytest import fixture, mark
from pytz import utc, timezone

from huskar_api.models.audit import AuditLog, action_creator, action_types
from huskar_api.models.audit.index import AuditIndexInstance
from huskar_api.models.auth import Authority
from ..utils import assert_response_ok


make_action = action_creator.make_action
LOCALHOST = '127.0.0.1'


@fixture
def test_audit(request, mocker, test_team, test_user, test_application):
    mocker.patch('huskar_api.extras.marshmallow.tzlocal', return_value=utc)

    actions = []

    if 'team' in request.param:
        actions.extend([
            (make_action(action_types.CREATE_TEAM, team=test_team), False),
            (make_action(action_types.DELETE_TEAM, team=test_team), True),
        ])
    if 'data' in request.param:
        kwargs = {
            'application_name': test_application.application_name,
            'cluster_name': 'stable',
            'key': '169.254.0.1_5000',
            'old_data': '{"foo": "bar"}',
            'new_data': '{"foo": "baz"}',
        }
        actions.extend([
            (make_action(action_types.UPDATE_SERVICE, **kwargs), False),
            (make_action(action_types.DELETE_SERVICE, **kwargs), True),
        ])

    audit_log = None
    for action, is_rollback in actions:
        rid = audit_log.id if is_rollback else None
        audit_log = AuditLog.create(
            test_user.id, LOCALHOST, action, rollback_to=rid)
        mocker.patch.object(
            audit_log, 'created_at', datetime.datetime(2012, 12, 12))
    return audit_log


@fixture
def prepare_instance_audit_logs(db, faker, mocker, test_user):
    def wrapped(instance_type, application_name, cluster_name, key,
                audit_num, created_at=None):
        action_type = getattr(action_types,
                              'UPDATE_%s' % instance_type.upper())
        extra = {
            'application_name': application_name,
            'cluster_name': cluster_name,
            'key': key,
        }
        new_data, old_data = faker.uuid4()[:8], None
        ids = []
        for i in range(audit_num):
            extra.update(old_data=old_data, new_data=new_data)
            action = make_action(action_type, **extra)
            with db.close_on_exit(False):
                audit_log = AuditLog.create(test_user.id, LOCALHOST, action)
                if created_at:
                    mocker.patch.object(audit_log, 'created_at', created_at)
                    instance_indicies = db.query(AuditIndexInstance).filter_by(
                        audit_id=audit_log.id).all()
                    for index in instance_indicies:
                        mocker.patch.object(index, 'created_at', created_at)
                ids.append(audit_log.id)
            new_data, old_data = faker.uuid4()[:8], new_data
        return ids
    return wrapped


@mark.parametrize('test_audit', ['team', 'team+data'], indirect=True)
def test_site_audit(client, test_user, test_team, test_audit, admin_token):
    r = client.get('/api/audit/site', headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert len(r.json['data']) == 2

    assert r.json['data'][1]['id'] > 0
    assert r.json['data'][1]['user']['username'] == test_user.username
    assert r.json['data'][1]['remote_addr'] == '127.0.0.1'
    assert r.json['data'][1]['action_name'] == 'CREATE_TEAM'
    assert json.loads(r.json['data'][1]['action_data']) == {
        'team_id': test_team.id,
        'team_name': test_team.team_name,
        'team_desc': test_team.team_desc,
    }
    assert r.json['data'][1]['created_at'] == '2012-12-12T00:00:00+00:00'
    assert r.json['data'][1]['rollback_to'] is None

    assert r.json['data'][0]['id'] > 0
    assert r.json['data'][0]['user'] == r.json['data'][1]['user']
    assert r.json['data'][0]['remote_addr'] == '127.0.0.1'
    assert r.json['data'][0]['action_name'] == 'DELETE_TEAM'
    assert r.json['data'][0]['action_data'] == r.json['data'][1]['action_data']
    assert r.json['data'][0]['created_at'] == r.json['data'][1]['created_at']
    assert r.json['data'][0]['rollback_to']['id'] == r.json['data'][1]['id']


@mark.parametrize('index_type', ['team', 'application'])
@mark.parametrize('test_audit', ['team+data', 'data'], indirect=True)
def test_team_and_application_audit(
        client, index_type, test_user, test_team, test_application,
        test_audit, admin_token):
    url = {
        'team': '/api/audit/team/%s' % test_team.team_name,
        'application': '/api/audit/application/%s' % (
            test_application.application_name),
    }[index_type]

    r = client.get(url, headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert len(r.json['data']) == 2

    assert r.json['data'][1]['id'] > 0
    assert r.json['data'][1]['user']['username'] == test_user.username
    assert r.json['data'][1]['remote_addr'] == '127.0.0.1'
    assert r.json['data'][1]['action_name'] == 'UPDATE_SERVICE'
    assert r.json['data'][1]['action_data']
    assert r.json['data'][1]['created_at'] == '2012-12-12T00:00:00+00:00'
    assert r.json['data'][1]['rollback_to'] is None

    assert r.json['data'][0]['id'] > 0
    assert r.json['data'][0]['user'] == r.json['data'][1]['user']
    assert r.json['data'][0]['remote_addr'] == '127.0.0.1'
    assert r.json['data'][0]['action_name'] == 'DELETE_SERVICE'
    assert r.json['data'][0]['action_data'] == r.json['data'][1]['action_data']
    assert r.json['data'][0]['created_at'] == r.json['data'][1]['created_at']
    assert r.json['data'][0]['rollback_to']['id'] == r.json['data'][1]['id']

    r = client.get(url + '1s', headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert r.json['data'] == []


@mark.parametrize('test_audit', ['team'], indirect=True)
def test_timezone(client, mocker, test_audit, admin_token):
    tz = timezone('Asia/Shanghai')
    mocker.patch('huskar_api.extras.marshmallow.tzlocal', return_value=tz)

    r = client.get('/api/audit/site', headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert len(r.json['data']) == 2
    assert r.json['data'][1]['created_at'] == '2012-12-12T00:00:00+08:06'
    assert r.json['data'][0]['created_at'] == r.json['data'][1]['created_at']


@mark.parametrize('test_audit', ['data'], indirect=True)
def test_sensitive_data(client, test_audit, test_application, admin_token):
    url = '/api/audit/application/%s' % test_application.application_name
    r = client.get(url, headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert len(r.json['data']) == 2
    assert r.json['data'][1]['action_data'] == (
        '{"application_name": "%s", "cluster_name": "stable", "data":'
        ' {"new": "{\\"foo\\": \\"baz\\"}", "old": "{\\"foo\\": \\"bar\\"}"}'
        ', "key": "169.254.0.1_5000"}'
    ) % test_application.application_name
    assert r.json['data'][0]['action_data'] == r.json['data'][1]['action_data']


def test_pagination(client, faker, test_user, test_team, admin_token):
    actions = itertools.chain.from_iterable(itertools.repeat([
        make_action(action_types.CREATE_TEAM, team=test_team),
        make_action(action_types.DELETE_TEAM, team=test_team),
    ], 50))
    audit_logs = [
        AuditLog.create(test_user.id, faker.ipv4(), action)
        for action in actions]
    audit_ids = [i.id for i in audit_logs]
    audit_ids.reverse()

    def request_with(query_string):
        r = client.get(
            '/api/audit/site', headers={'Authorization': admin_token},
            query_string=query_string)
        assert_response_ok(r)
        return r.json['data']

    d = request_with(None)
    assert len(d) == 100
    assert [i['id'] for i in d] == audit_ids[:100]

    d = request_with({'start': -1})
    assert len(d) == 100
    assert [i['id'] for i in d] == audit_ids[:100]

    d = request_with({'start': 10})
    assert len(d) == 90
    assert [i['id'] for i in d] == audit_ids[10:]

    d = request_with({'start': 30})
    assert len(d) == 100 - 30
    assert [i['id'] for i in d] == audit_ids[30:]

    d = request_with({'start': 100})
    assert len(d) == 0
    assert [i['id'] for i in d] == []


@mark.parametrize('test_audit', ['data'], indirect=True)
@mark.xparametrize
def test_fetch_with_date(
        client, test_user, test_team, test_audit, admin_token,
        timedelta, result_len):

    def request_with(query_string):
        r = client.get(
            '/api/audit/team/{}'.format(test_team.team_name),
            headers={'Authorization': admin_token},
            query_string=query_string)
        assert_response_ok(r)
        return r.json['data']

    today = datetime.date.today()
    date = (today + datetime.timedelta(days=timedelta)).strftime('%Y-%m-%d')
    query_string = {'date': date}
    data = request_with(query_string)
    assert len(data) == result_len


@mark.xparametrize
def test_get_audit_instance_timeline(
        client, faker, test_application, test_application_token,
        instance_type, cluster_name, instance_key, prepare_data,
        expected_audit_num, prepare_instance_audit_logs):
    for data in prepare_data:
        created_at = datetime.datetime.strptime(
            data['created_date'], '%Y-%m-%d')
        prepare_instance_audit_logs(
            instance_type, test_application.application_name, cluster_name,
            instance_key, created_at=created_at, audit_num=data['audit_num'])
    url = '/api/audit-timeline/{}/{}/{}/{}'.format(
        instance_type, test_application.application_name, cluster_name,
        instance_key)
    r = client.get(url, headers={'Authorization': test_application_token})
    assert r.status_code == 200
    assert len(r.json['data']) == expected_audit_num


INCLUDE_DATA_TYPES = [
    action_types.UPDATE_SERVICE,
    action_types.DELETE_SERVICE,
    action_types.UPDATE_SWITCH,
    action_types.DELETE_SWITCH,
    action_types.UPDATE_CONFIG,
    action_types.DELETE_CONFIG,
    action_types.IMPORT_SERVICE,
    action_types.IMPORT_SWITCH,
    action_types.IMPORT_CONFIG,
    action_types.UPDATE_SERVICE_INFO,
    action_types.UPDATE_CLUSTER_INFO,
    action_types.UPDATE_CLUSTER_INFO,
]


@mark.parametrize('test_url', [
    '/api/audit/site',
    '/api/audit/team/%(team)s',
    '/api/audit/application/%(application)s',
    '/api/audit-timeline/service/%(application)s/stable/169.254.0.1_5000',
])
def test_no_permission_check(
        client, test_url, test_team, test_application, test_token):
    url = test_url % {
        'team': test_team.team_name,
        'application': test_application.application_name,
    }
    r = client.get(url, headers={'Authorization': test_token})
    assert_response_ok(r)


@mark.parametrize('test_audit', ['data+team'], indirect=True)
@mark.parametrize('test_url', [
    '/api/audit/site',
    '/api/audit/team/%(team)s',
    '/api/audit/application/%(application)s',
    '/api/audit-timeline/service/%(application)s/stable/169.254.0.1_5000',
])
def test_site_admin_can_view_all_sensitive_data(
        client, test_url, test_team, test_audit,
        test_application, admin_token):
    url = test_url % {
        'team': test_team.team_name,
        'application': test_application.application_name,
    }
    r = client.get(url, headers={'Authorization': admin_token})
    assert_response_ok(r)
    for action_log in r.json['data']:
        if action_log['action_name'] in INCLUDE_DATA_TYPES:
            assert 'data' in action_log['action_data']


@mark.parametrize('test_audit', ['data+team'], indirect=True)
@mark.parametrize('test_url,can_view', [
    ('/api/audit/site', False),
    ('/api/audit/team/%(team)s', True),
    ('/api/audit/application/%(application)s', True),
    ('/api/audit-timeline/service/%(application)s/stable/169.254.0.1_5000',
     True)
])
def test_team_admin_can_view_team_sensitive_data(
        client, test_url, test_team, test_audit, test_user,
        test_application, test_token, can_view):
    test_team.grant_admin(test_user.id)

    url = test_url % {
        'team': test_team.team_name,
        'application': test_application.application_name,
    }
    r = client.get(url, headers={'Authorization': test_token})
    assert_response_ok(r)
    for action_log in r.json['data']:
        if action_log['action_name'] in INCLUDE_DATA_TYPES:
            if can_view:
                assert 'data' in action_log['action_data']
            else:
                assert 'data' not in action_log['action_data']


@mark.parametrize('test_audit', ['data+team'], indirect=True)
@mark.parametrize('test_url,can_view', [
    ('/api/audit/site', False),
    ('/api/audit/team/%(team)s', False),
    ('/api/audit/application/%(application)s', True),
    ('/api/audit-timeline/service/%(application)s/stable/169.254.0.1_5000',
     True)
])
def test_read_auth_can_view_application_sensitive_data(
        client, test_url, test_team, test_audit, test_user,
        test_application, test_token, can_view):
    test_application.ensure_auth(Authority.READ, test_user.id)

    url = test_url % {
        'team': test_team.team_name,
        'application': test_application.application_name,
    }
    r = client.get(url, headers={'Authorization': test_token})
    assert_response_ok(r)
    for action_log in r.json['data']:
        if action_log['action_name'] in INCLUDE_DATA_TYPES:
            if can_view:
                assert 'data' in action_log['action_data']
            else:
                assert 'data' not in action_log['action_data']


@mark.parametrize('test_audit', ['data+team'], indirect=True)
@mark.parametrize('test_url', [
    '/api/audit/site',
    '/api/audit/team/%(team)s',
    '/api/audit/application/%(application)s',
    '/api/audit-timeline/service/%(application)s/stable/169.254.0.1_5000',
])
def test_normal_user_cant_view_sensitive_data(
        client, test_url, test_team, test_audit,
        test_application, test_token):
    url = test_url % {
        'team': test_team.team_name,
        'application': test_application.application_name,
    }
    r = client.get(url, headers={'Authorization': test_token})
    assert_response_ok(r)
    for action_log in r.json['data']:
        assert 'data' not in action_log['action_data']
