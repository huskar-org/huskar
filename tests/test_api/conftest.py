from __future__ import absolute_import

import json

from pytest import fixture

from huskar_api.models.auth import User, Team, Application
from huskar_api.models.audit import AuditLog, action_types
from huskar_api.models.const import SELF_APPLICATION_NAME
from huskar_api.models.webhook import Webhook
from huskar_api import settings


@fixture(scope='function')
def admin_user(db):
    admin_user = User.create_normal('admin', password='admin', is_active=True)
    admin_user.grant_admin()
    return admin_user


@fixture(scope='function')
def admin_token(client, admin_user):
    return admin_user.generate_token(settings.SECRET_KEY, expires_in=3600)


@fixture(scope='function')
def test_user(faker):
    email = faker.safe_email()
    username = email.split('@', 1)[0]
    password = faker.password()
    return User.create_normal(username, password, email, is_active=True)


@fixture(scope='function')
def test_token(test_user):
    return test_user.generate_token(settings.SECRET_KEY, expires_in=3600)


@fixture(scope='function')
def test_team(db, faker):
    team_name = 'test_%s' % faker.uuid4()[:8]
    return Team.create(team_name)


@fixture(scope='function')
def test_application(db, faker, test_team):
    application_name = faker.uuid4()[:8]
    return Application.create(application_name, test_team.id)


@fixture(scope='function')
def self_application(db, faker, test_team):
    return Application.create(SELF_APPLICATION_NAME, test_team.id)


@fixture(params=['foo', 'bar'], scope='function')
def test_application_fallback_token(request, db, test_application):
    user = User.get_by_name(test_application.application_name)
    return user.generate_token(request.param)


@fixture(scope='function')
def test_application_token(db, test_application):
    user = User.get_by_name(test_application.application_name)
    return user.generate_token(settings.SECRET_KEY)


@fixture(scope='function')
def stolen_application(db, test_team, test_application):
    application_name = 'stolen-%s' % test_application.application_name
    return Application.create(application_name, test_team.id)


@fixture(scope='function')
def stolen_application_token(db, test_team, stolen_application):
    user = User.get_by_name(stolen_application.application_name)
    return user.generate_token(settings.SECRET_KEY)


@fixture
def last_audit_log(db):
    def _last_audit_log():
        db.rollback()  # Hmm... The repeated reading issue
        instance = db.query(AuditLog).order_by(AuditLog.id.desc()).first()
        if instance is not None:
            instance.action_json = json.loads(instance.action_data)
        return instance
    return _last_audit_log


@fixture(scope='function')
def universal_webhook():
    return Webhook.create('http://universal.foo.bar', Webhook.TYPE_UNIVERSAL)


@fixture(scope='function')
def add_webhook_subscriptions(test_application):
    webhook = Webhook.create('http://foo.foo.bar')
    for action_name in action_types._action_map:
        action_type = getattr(action_types, action_name)
        webhook.subscribe(test_application.id, action_type)


@fixture(scope='function')
def webhook_backends(monkeypatch, universal_webhook):
    backends = []

    def mock_callback(url, data, *args, **kwargs):
        backends.append(data)

    monkeypatch.setattr(
        'huskar_api.models.webhook.notify.notify_callback',
        mock_callback
    )
    yield backends
    monkeypatch.undo()


@fixture(scope='function')
def client_ip(client, faker):
    ip = faker.ipv4()
    client.environ_base['REMOTE_ADDR'] = ip
    try:
        yield ip
    finally:
        client.environ_base['REMOTE_ADDR'] = '127.0.0.1'
