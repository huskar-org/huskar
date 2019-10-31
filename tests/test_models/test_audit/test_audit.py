from __future__ import absolute_import

import copy
import json
import datetime

from pytest import fixture, mark, raises
from sqlalchemy.exc import InternalError

from huskar_api import settings
from huskar_api.models import DBSession
from huskar_api.models.auth import User, Team, Application, Authority
from huskar_api.models.audit import AuditLog, action_creator, action_types
from huskar_api.models.audit.const import (
    TYPE_SITE, NORMAL_USER, APPLICATION_USER,
    SEVERITY_DANGEROUS, SEVERITY_NORMAL)
from huskar_api.models.audit.index import AuditIndex
from huskar_api.models.signals import new_action_detected
from huskar_api.models.exceptions import (
    AuditLogTooLongError, AuditLogLostError)


@fixture
def user():
    return User.create_normal('foo', '-', 'foo@example.com')


@fixture
def team():
    return Team.create('foobar')


@fixture
def application(team):
    return Application.create('base.foo', team.id)


@fixture
def action(team):
    return action_creator.make_action(action_types.CREATE_TEAM, team=team)


@fixture
def large_action(request, application):
    extra = {
        'application_name': application.application_name,
        'cluster_name': 'stable',
        'key': '169.254.0.1_5000',
        'old_data': 'x' * request.param,
        'new_data': 'y' * request.param,
    }
    return action_creator.make_action(action_types.UPDATE_SERVICE, **extra)


@fixture
def unicode_action(application):
    extra = {
        'application_name': application.application_name,
        'cluster_name': 'bar',
        'key': 'test',
        'old_data': u'\u86e4',
        'new_data': u'\u87c6',
    }
    return action_creator.make_action(action_types.UPDATE_CONFIG, **extra)


@fixture
def config_action():
    extra = {
        'application_name': 'base.foo',
        'cluster_name': 'foo',
        'key': 'bar'
    }
    return action_creator.make_action(action_types.UPDATE_CONFIG, **extra)


@fixture
def connect_new_action_detected():
    new_func_list = []

    def wrapper(func):
        new_func = new_action_detected.connect_via(AuditLog)(func)
        new_func_list.append(new_func)
        return new_func

    try:
        yield wrapper
    finally:
        for func in new_func_list:
            new_action_detected.disconnect(func)


@mark.parametrize('can_rollback', [False, True])
def test_audit_log_creation(faker, user, team, action, can_rollback):
    assert AuditIndex.get_audit_ids(TYPE_SITE, 0) == []

    last_audit_log = AuditLog.create(user.id, faker.ipv4(), action)
    if can_rollback:
        audit_log = AuditLog.create(
            user.id, faker.ipv4(), action, last_audit_log.id)
    else:
        audit_log = AuditLog.create(user.id, faker.ipv4(), action)
    assert audit_log.user_id == user.id
    assert audit_log.user is user
    assert audit_log.action_type == action_types.CREATE_TEAM
    assert audit_log.action_name == 'CREATE_TEAM'
    assert json.loads(audit_log.action_data) == {
        'team_id': team.id,
        'team_name': team.team_name,
        'team_desc': team.team_desc,
    }
    if can_rollback:
        assert audit_log.rollback_id == last_audit_log.id
        assert audit_log.rollback_to is last_audit_log

    with raises(ValueError) as error:
        AuditLog.create(user.id, faker.ipv4(), action, -1)
    assert error.match(r'^rollback_to is not a valid id$')

    assert AuditIndex.get_audit_ids(TYPE_SITE, 0) == \
        [audit_log.id, last_audit_log.id]
    today = datetime.date.today()
    assert AuditIndex.get_audit_ids_by_date(TYPE_SITE, 0, today) == \
        [audit_log.id, last_audit_log.id]


def test_create_too_large_audit(faker, mocker, user):
    mocker.patch('huskar_api.models.audit.audit._publish_new_action')
    action_data = 'x' * 65528
    action = (action_types.UPDATE_CONFIG, action_data, [(TYPE_SITE, 0)])
    with raises(AuditLogTooLongError):
        AuditLog.create(user.id, faker.ipv4(), action)

    action_data = 'x' * 65527
    action = (action_types.UPDATE_CONFIG, action_data, [(TYPE_SITE, 0)])
    assert AuditLog.create(user.id, faker.ipv4(), action)


def test_create_lost_audit(faker, mocker, user):
    hook = mocker.patch('huskar_api.models.audit.audit._publish_new_action')
    session = mocker.patch('huskar_api.models.audit.audit.DBSession')
    session.side_effect = [InternalError(None, None, None, None)]
    action = (action_types.UPDATE_CONFIG, 'data', [(TYPE_SITE, 0)])
    with raises(AuditLogLostError):
        AuditLog.create(user.id, faker.ipv4(), action)

    hook.assert_called_once()


def test_audit_event_publish_failed(
        faker, mocker, user, action, connect_new_action_detected):
    capture_exception = mocker.patch(
        'huskar_api.models.audit.audit.capture_exception',
        autospec=True
    )

    @connect_new_action_detected
    def raise_error(*args, **kwargs):
        raise ValueError

    AuditLog.create(user.id, faker.ipv4(), action)
    assert capture_exception.called


def test_prefetch_audit_log(faker, action):
    users = [
        User.create_normal(faker.uuid4()[:8], '-', faker.email())
        for _ in xrange(10)]
    audit_logs = [
        AuditLog.create(user.id, faker.ipv4(), action) for user in users]
    rollback_logs = [
        AuditLog.create(
            audit.user_id, faker.ipv4(), action, rollback_to=audit.id)
        for audit in audit_logs]
    ids = [audit.id for audit in audit_logs + rollback_logs]
    fetched_logs = AuditLog.get_multi_and_prefetch(ids)

    for index, audit in enumerate(fetched_logs[:len(audit_logs)]):
        assert audit.__dict__['user'] is users[index]
        assert 'rollback_to' not in audit.__dict__
        assert audit.user is users[index]
        assert audit.rollback_to is None

    for index, audit in enumerate(fetched_logs[len(audit_logs):]):
        assert audit.__dict__['user'] is users[index]
        assert audit.__dict__['rollback_to'] is audit_logs[index]
        assert audit.user is users[index]
        assert audit.rollback_to is audit_logs[index]


def test_get_multi_by_index(faker, action):
    assert AuditLog.get_multi_by_index(AuditLog.TYPE_SITE, 0)[:] == []
    assert AuditLog.get_multi_by_index(AuditLog.TYPE_TEAM, 0)[:] == []
    user = User.create_normal(faker.uuid4(), '-', faker.email())
    log = AuditLog.create(user.id, faker.ipv4(), action)
    assert AuditLog.get_multi_by_index(AuditLog.TYPE_SITE, 0)[:] == [log]
    assert AuditLog.get_multi_by_index(AuditLog.TYPE_TEAM, 0)[:] == []


@mark.parametrize('large_action,old,new', [
    (1024, 'x' * 1024, 'y' * 1024),
    (1025, 'x' * 1025, 'y' * 1025),
    (5, 'x' * 5, 'y' * 5),
], indirect=['large_action'], ids=lambda args: args[0])
def test_large_data(user, faker, action, large_action, old, new):
    audit_log = AuditLog.create(user.id, faker.ipv4(), large_action)
    action_data = json.loads(audit_log.action_data)
    assert action_data['data'].get('old') == old
    assert action_data['data'].get('new') == new


def test_unicode_data(user, faker, action, unicode_action):
    audit_log = AuditLog.create(user.id, faker.ipv4(), unicode_action)
    action_data = json.loads(audit_log.action_data)
    test_data = u'{old}{new}'.format(**action_data['data'])
    assert test_data == u'\u86e4\u87c6'


def test_get_multi_by_indices_with_date(faker, user, action):
    log = AuditLog.create(user.id, faker.ipv4(), action)
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    assert AuditLog.get_multi_by_index_with_date(
        AuditLog.TYPE_SITE, 0, today)[:] == [log]
    assert AuditLog.get_multi_by_index_with_date(
        AuditLog.TYPE_SITE, 0, yesterday)[:] == []


def test_rollback(user, unicode_action, action, config_action,
                  mocker, faker, zk):
    audit_log = AuditLog.create(user.id, faker.ipv4(), action)
    assert audit_log.can_rollback is False
    audit_log = AuditLog.create(user.id, faker.ipv4(), unicode_action)
    assert audit_log.can_rollback is True

    audit_log = AuditLog.create(user.id, faker.ipv4(), config_action)
    _, action_data, _ = config_action
    zk.ensure_path('/huskar/config/%s/%s/%s' % (
        action_data['application_name'], action_data['cluster_name'],
        action_data['key']))
    instance = audit_log.rollback(user.id, faker.ipv4())
    assert instance.rollback_to == audit_log


def test_get_multi_by_instance_index(
        db, redis_client, redis_flushall, faker, user, application):
    cluster_name = 'bar'
    key = 'test'
    action_type = action_types.UPDATE_CONFIG
    _, data_type = action_types[action_type].split('_', 1)
    data_type = data_type.lower()
    audit_num = 3

    for _ in range(audit_num):
        extra = {
            'application_name': application.application_name,
            'cluster_name': 'bar',
            'key': 'test',
            'old_data': 'old',
            'new_data': 'new'
        }
        action = action_creator.make_action(
            action_types.UPDATE_CONFIG, **extra)
        AuditLog.create(user.id, faker.ipv4(), action)

    audit_logs = AuditLog.get_multi_by_instance_index(
        AuditLog.TYPE_CONFIG, application.id, cluster_name, key)
    assert len(audit_logs[:]) == audit_num

    with DBSession().close_on_exit(False):
        DBSession.delete(user)
    User._db_session.close()
    redis_flushall(redis_client)
    audit_logs = AuditLog.get_multi_by_instance_index(
        AuditLog.TYPE_CONFIG, application.id, cluster_name, key)
    assert not any(getattr(x, 'user') for x in audit_logs)


@mark.parametrize('target_type', [
    AuditLog.TYPE_SITE,
    AuditLog.TYPE_TEAM,
    AuditLog.TYPE_APPLICATION,
    AuditLog.TYPE_CONFIG,
    AuditLog.TYPE_SWITCH,
    AuditLog.TYPE_SERVICE,
])
def test_site_admin_can_view_all_sensitive_data(user, target_type):
    user.grant_admin()
    assert AuditLog.can_view_sensitive_data(user.id, target_type, None)


@mark.parametrize('target_type,can_view', [
    (AuditLog.TYPE_SITE, False),
    (AuditLog.TYPE_TEAM, True),
    (AuditLog.TYPE_APPLICATION, True),
    (AuditLog.TYPE_CONFIG, True),
    (AuditLog.TYPE_SWITCH, True),
    (AuditLog.TYPE_SERVICE, True),
])
def test_application_admin_can_view_team_sensitive_data(
        user, application, team, target_type, can_view):
    team.grant_admin(user.id)

    if target_type == AuditLog.TYPE_SITE:
        target_id = 0
    elif target_type == AuditLog.TYPE_TEAM:
        target_id = team.id
    else:
        target_id = application.id
    result = AuditLog.can_view_sensitive_data(user.id, target_type, target_id)
    assert result == can_view


@mark.parametrize('authority', [Authority.READ, Authority.WRITE])
@mark.parametrize('target_type,can_view', [
    (AuditLog.TYPE_SITE, False),
    (AuditLog.TYPE_TEAM, False),
    (AuditLog.TYPE_APPLICATION, True),
    (AuditLog.TYPE_CONFIG, True),
    (AuditLog.TYPE_SWITCH, True),
    (AuditLog.TYPE_SERVICE, True),
])
def test_application_auth_can_view_application_sensitive_data(
        user, application, team, target_type, can_view, authority):
    application.ensure_auth(authority, user.id)
    if target_type == AuditLog.TYPE_SITE:
        target_id = 0
    elif target_type == AuditLog.TYPE_TEAM:
        target_id = team.id
    else:
        target_id = application.id
    result = AuditLog.can_view_sensitive_data(user.id, target_type, target_id)
    assert result == can_view


@mark.parametrize('target_type', [
    AuditLog.TYPE_SITE,
    AuditLog.TYPE_TEAM,
    AuditLog.TYPE_APPLICATION,
    AuditLog.TYPE_CONFIG,
    AuditLog.TYPE_SWITCH,
    AuditLog.TYPE_SERVICE,
])
def test_normal_user_can_not_view_sensitive_data(
        user, application, team, target_type):
    if target_type == AuditLog.TYPE_SITE:
        target_id = 0
    elif target_type == AuditLog.TYPE_TEAM:
        target_id = team.id
    else:
        target_id = application.id

    assert not AuditLog.can_view_sensitive_data(
        user.id, target_type, target_id)


@mark.parametrize('data', [True, False])
@mark.parametrize('value', [True, False])
@mark.parametrize('nested', [True, False])
def test_desensitize(user, action, data, value, nested):
    audit_log = AuditLog.create(user.id, '127.0.0.1', action)
    action_data = json.loads(audit_log.action_data)
    if data:
        action_data['data'] = {'old_data': '233', 'new_data': '666'}
    if value:
        action_data['value'] = {'url': 'sam+redis://redis.111/test'}
    if nested:
        action_data['nested'] = {
            'client_self_check': {'overall': {'test': '0'}}}
    audit_log.action_data = json.dumps(action_data)

    action_data = audit_log.desensitize()['action_data']
    action_data = json.loads(action_data)
    for key in ('data', 'value', 'nested'):
        assert key not in action_data


def test_fix_create_action_value_error(
        mocker, application, user, connect_new_action_detected):
    extra = {
        'application_name': 'foo.bar',
        'cluster_name': 'alta',
        'key':
            'b1b800dae3fca57fcb429615ff3e0a7054c59206640f1ef4dd30c12eccfdde43',
        'new_data': {
            'cluster': 'alta',
            'ip': '192.168.1.2',
            'meta': {'state': 'up'},
        },
        'old_data': None,
    }
    mocker.patch.object(
        Application, 'get_by_name', side_effect=[None, application])
    events = []

    @connect_new_action_detected
    def test_event(sender, action_type, username, user_type,
                   action_data, is_subscriable, severity):
        events.append([
            action_type, username, user_type, action_data, is_subscriable,
            severity])

    action = action_creator.make_action(action_types.UPDATE_SERVICE, **extra)
    assert len(action.action_indices) == 1
    assert len(action.action_indices[0]) == 4
    AuditLog.create(user.id, '127.0.0.1', action)

    assert len(events) == 1
    action_type, _, user_type, _, is_subscriable, severity = events[0]
    assert not is_subscriable
    assert severity == SEVERITY_DANGEROUS
    assert user_type == NORMAL_USER
    assert action_type == action_types.UPDATE_SERVICE


@mark.parametrize('is_application', [False, True])
def test_application_use_user_token_change_user_type(
        is_application, mocker, application, user,
        connect_new_action_detected):
    extra = {
        'application_name': application.application_name,
        'cluster_name': 'alta',
        'key':
            'b1b800dae3fca57fcb429615ff3e0a7054c59206640f1ef4dd30c12eccfdde43',
        'new_data': {
            'cluster': 'alta',
            'ip': '192.168.1.2',
            'meta': {'state': 'up'},
        },
        'old_data': None,
    }
    if is_application:
        mocker.patch.object(
            settings, 'APPLICATION_USE_USER_TOKEN_USER_LIST', [user.username])
    events = []

    @connect_new_action_detected
    def test_event(sender, action_type, username, user_type,
                   action_data, is_subscriable, severity):
        events.append([
            action_type, username, user_type, action_data, is_subscriable,
            severity])

    action = action_creator.make_action(action_types.UPDATE_SERVICE, **extra)
    AuditLog.create(user.id, '127.0.0.1', action)

    assert len(events) == 1
    action_type, _, user_type, _, is_subscriable, severity = events[0]
    assert is_subscriable
    assert severity == SEVERITY_DANGEROUS
    if is_application:
        assert user_type == APPLICATION_USER
    else:
        assert user_type == NORMAL_USER
    assert action_type == action_types.UPDATE_SERVICE


def test_publish_new_action_with_low_severity(
        mocker, user, connect_new_action_detected):
    extra = {
        'user': user,
    }
    events = []

    @connect_new_action_detected
    def test_event(sender, action_type, username, user_type,
                   action_data, is_subscriable, severity):
        events.append([
            action_type, username, user_type, action_data, is_subscriable,
            severity])

    action = action_creator.make_action(
        action_types.OBTAIN_USER_TOKEN, **extra)
    AuditLog.create(user.id, '127.0.0.1', action)

    assert len(events) == 1
    action_type, _, user_type, _, is_subscriable, severity = events[0]
    assert not is_subscriable
    assert user_type == NORMAL_USER
    assert severity == SEVERITY_NORMAL
    assert action_type == action_types.OBTAIN_USER_TOKEN


def test_update_dangerous_action_names_exclude_list():
    old_data = copy.copy(settings.DANGEROUS_ACTION_NAMES_EXCLUDE_LIST)
    new_data = frozenset(['UPDATE_SERVICE', 'UPDATE_CONFIG'])
    try:
        assert old_data != new_data
        settings.update_dangerous_action_names_exclude_list(new_data)
        assert settings.DANGEROUS_ACTION_NAMES_EXCLUDE_LIST == new_data
    finally:
        settings.DANGEROUS_ACTION_NAMES_EXCLUDE_LIST = old_data


def test_update_application_use_user_token_user_list():
    old_data = copy.copy(settings.APPLICATION_USE_USER_TOKEN_USER_LIST)
    new_data = frozenset(['test', 'admin'])
    try:
        assert old_data != new_data
        settings.update_application_use_user_token_user_list(new_data)
        assert settings.APPLICATION_USE_USER_TOKEN_USER_LIST == new_data
    finally:
        settings.APPLICATION_USE_USER_TOKEN_USER_LIST = old_data


def test_trace_all_application_events(
        mocker, application, user, monitor_client):
    extra = {
        'application_name': application.application_name,
        'cluster_name': 'alta',
        'key':
            'b1b800dae3fca57fcb429615ff3e0a7054c59206640f1ef4dd30c12eccfdde43',
        'new_data': {
            'cluster': 'alta',
            'ip': '192.168.1.2',
            'meta': {'state': 'up'},
        },
        'old_data': None,
    }

    action = action_creator.make_action(action_types.UPDATE_SERVICE, **extra)
    AuditLog.create(user.id, '127.0.0.1', action)

    monitor_client.increment.assert_called_once_with(
        'audit.application_event', tags={
            'action_name': 'UPDATE_SERVICE',
            'application_name': application.application_name,
        })
