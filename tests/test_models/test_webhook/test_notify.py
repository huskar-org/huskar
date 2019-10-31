from __future__ import absolute_import

import hashlib

import gevent
from pytest import fixture, mark
from requests import Response, Session
from requests.cookies import cookiejar_from_dict
from requests.exceptions import Timeout, ConnectionError, HTTPError

from huskar_api.models.webhook.notify import Notifier, switch
from huskar_api.models.webhook.webhook import Webhook
from huskar_api.models.audit.action import action_types
from huskar_api.models.audit.const import (
    NORMAL_USER, SEVERITY_NORMAL, SEVERITY_DANGEROUS)
from huskar_api.models.auth import Application, Team
from huskar_api.models.signals import new_action_detected


@fixture
def test_team(db):
    team = Team(team_name='foo', team_desc='foo-test')
    db.add(team)
    db.commit()
    return team


@fixture
def test_application(db, test_team):
    app = Application(application_name='foo.bar', team_id=test_team.id)
    db.add(app)
    db.commit()
    return app


@fixture
def notifier():
    notifier = Notifier()
    notifier.start()
    yield notifier
    notifier._running = False


@fixture
def test_webhook(db):
    hook = Webhook(url='http://www.foo.me', hook_type=Webhook.TYPE_NORMAL)
    db.add(hook)
    db.commit()
    return hook


@fixture
def mock_response():
    def make_response(url='', content='ok', status_code=200, history=(),
                      encoding='utf8', reason='', cookies=None):
        r = Response()
        r.url = url
        r._content = content
        r.status_code = status_code
        r.history = history
        r.encoding = encoding
        r.reason = reason
        r.cookies = cookiejar_from_dict(cookies or {})
        return r

    return make_response


@fixture
def mock_logger(mocker):
    logger = mocker.patch(
        'huskar_api.models.webhook.notify.logger',
        autospec=True)
    return logger


@fixture
def mock_bad_callback(mocker):
    def make_mock(error):
        mocker.patch(
            'huskar_api.models.webhook.notify.session.request',
            side_effect=error
        )
    return make_mock


@fixture
def normal_webhooks(test_application):
    url_list = [
        'http://n.foo.bar',
        'http://n.foo.me'
    ]
    webhooks = [Webhook.create(url) for url in url_list]
    for webhook in webhooks:
        for action_name in action_types._action_map:
            action_type = getattr(action_types, action_name)
            webhook.subscribe(test_application.id, action_type)

    return webhooks


@fixture
def universal_webhooks():
    url_list = [
        'http://u.foo.bar',
        'http://u.foo.me'
    ]
    return [
        Webhook.create(url, hook_type=Webhook.TYPE_UNIVERSAL)
        for url in url_list
    ]


def subscribe_all(webhook, application_id):
    for name in action_types._subscriable_types:
        action_type = action_types._action_map[name]
        webhook.subscribe(application_id, action_type)


def test_dispatch_event(
        mocker, test_webhook, notifier, test_application, mock_response):
    user_name = 'foo'
    application_name = test_application.application_name
    action_type = action_types.CREATE_CONFIG_CLUSTER
    test_webhook.subscribe(test_application.id, action_type)
    with mocker.patch.object(Session, 'request'):
        notifier.publish([
            application_name], user_name, NORMAL_USER, action_type)
        assert notifier.hook_queue.qsize() != 0


def test_notifier_start(test_webhook):
    notifier = Notifier()
    assert notifier._running is False
    notifier.start()
    assert notifier._running

    worker = notifier._worker
    notifier.start()
    assert notifier._running
    assert id(worker) == id(notifier._worker)


@mark.parametrize('severity', [SEVERITY_NORMAL, SEVERITY_DANGEROUS])
def test_publish_universal(
        test_webhook, test_application, universal_webhooks,
        mock_response, faker, mocker, mock_logger, severity):
    notifier = Notifier()
    for action_name in action_types._action_map:
        action_type = getattr(action_types, action_name)
        action_data = {'fake_msg': faker.paragraph()}
        notifier.publish_universal(
            action_type, faker.first_name(), NORMAL_USER, action_data,
            severity=severity)
    assert notifier.hook_queue.qsize() == (
        len(universal_webhooks) * len(action_types._action_map))

    total = len(universal_webhooks) * len(action_types._action_map)
    request = mocker.patch.object(
        Session, 'request', return_value=mock_response())
    notifier.start()
    assert notifier.hook_queue.qsize() != 0
    gevent.sleep(0.1)
    assert notifier.hook_queue.qsize() == 0

    assert request.call_count == total
    assert request.call_args[1]['json']['application_names'] == []
    assert request.call_args[1]['json']['severity'] == severity
    assert mock_logger.exception.call_count == 0


def test_publish_switch_off(
        universal_webhooks, test_application, mocker, faker):
    with mocker.patch.object(switch, 'is_switched_on', return_value=False):
        notifier = Notifier()
        action_type = action_types.UPDATE_CONFIG
        action_data = {'application_name': faker.uuid4()[:8]}
        notifier.publish_universal(
            action_type, faker.first_name(), NORMAL_USER, action_data,
            severity=1)
        assert notifier.hook_queue.qsize() == 0


@mark.parametrize('severity', [SEVERITY_NORMAL, SEVERITY_DANGEROUS])
def test_publish_normal(test_webhook, test_application, normal_webhooks,
                        mock_response, faker, mocker, mock_logger,
                        severity):
    notifier = Notifier()
    application_name = test_application.application_name
    user_name = 'foo'
    for action_name in action_types._action_map:
        action_type = getattr(action_types, action_name)
        action_data = {'application_name': faker.uuid4()[:8]}
        notifier.publish([application_name], user_name, NORMAL_USER,
                         action_type, action_data, severity=severity)
    assert notifier.hook_queue.qsize() == (
        len(normal_webhooks) * len(action_types._action_map))

    request = mocker.patch.object(Session, 'request',
                                  return_value=mock_response())
    notifier.start()
    assert notifier.hook_queue.qsize() != 0
    gevent.sleep(0.1)
    assert notifier.hook_queue.qsize() == 0

    total = len(normal_webhooks) * len(action_types._action_map)
    assert request.call_count == total
    assert request.call_args[1]['json']['severity'] == severity
    assert mock_logger.exception.call_count == 0


def test_catch_unexpected_callback_error(
        faker, mock_bad_callback, universal_webhooks, mock_logger):
    mock_bad_callback(ValueError)
    notifier = Notifier()
    action_type = getattr(action_types, 'CREATE_CONFIG_CLUSTER')
    action_data = {'application_name': faker.uuid4()[:8]}
    notifier.publish_universal(
        action_type, faker.first_name(), NORMAL_USER, action_data,
        severity=1)
    assert notifier.hook_queue.qsize() == len(universal_webhooks)

    notifier.start()
    assert notifier.hook_queue.qsize() != 0
    gevent.sleep(0.1)
    assert notifier.hook_queue.qsize() == 0

    assert mock_logger.exception.call_count == len(universal_webhooks)


@mark.parametrize('error', [Timeout, ConnectionError, HTTPError])
def test_catch_remote_error(faker, mock_bad_callback, universal_webhooks,
                            mock_logger, error):
    mock_bad_callback(error)
    notifier = Notifier()
    action_type = getattr(action_types, 'CREATE_CONFIG_CLUSTER')
    action_data = {'application_name': faker.uuid4()[:8]}
    notifier.publish_universal(
        action_type, faker.first_name(), NORMAL_USER, action_data,
        severity=1)
    assert notifier.hook_queue.qsize() == len(universal_webhooks)

    notifier.start()
    assert notifier.hook_queue.qsize() != 0
    gevent.sleep(0.1)
    assert notifier.hook_queue.qsize() == 0
    url_summary = hashlib.md5('http://u.foo.me').hexdigest()
    mock_logger.warn.assert_called_with(
        'Remote Request Failed: %s, %s', url_summary, str(error()))
    assert mock_logger.warn.call_count == len(universal_webhooks)


def test_issue_430_key_error(
        mocker, faker, universal_webhooks, normal_webhooks):
    action_type = getattr(action_types, 'DELETE_USER')
    action_data = {'application_names': [faker.uuid4()[:8]]}
    notify_add_mock = mocker.patch('huskar_api.models.webhook.notifier._add')
    new_action_detected.send(
        'audit_log',
        action_type=action_type,
        username=faker.first_name(),
        user_type=0,
        action_data=action_data,
        is_subscriable=True,
        severity=1
    )
    assert notify_add_mock.call_count != 0
