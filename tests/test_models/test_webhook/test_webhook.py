from __future__ import absolute_import

from pytest import fixture
from sqlalchemy.exc import IntegrityError

from huskar_api.models.auth import Team, Application
from huskar_api.models.webhook import Webhook
from huskar_api.models.webhook.webhook import WebhookSubscription
from huskar_api.models.audit import action_types


@fixture
def test_team(db):
    team = Team(team_name='foo', team_desc='foo-test')
    db.add(team)
    db.commit()
    return team


@fixture
def test_application(db, test_team):
    app = Application(application_name='bar.foo', team_id=test_team.id)
    db.add(app)
    db.commit()
    return app


@fixture
def test_webhook(db):
    hook = Webhook(
        url='http://www.foo.me',
        hook_type=Webhook.TYPE_NORMAL)
    db.add(hook)
    db.commit()
    return hook


def test_create_webhook(db):
    instance = Webhook.create('http://www.foo.bar')
    assert instance.id > 0
    assert instance.url == 'http://www.foo.bar'
    assert instance.hook_type == Webhook.TYPE_NORMAL
    assert instance.is_normal

    instance = Webhook.create('http://foo.foo.bar', Webhook.TYPE_UNIVERSAL)
    assert instance.id > 0
    assert instance.url == 'http://foo.foo.bar'
    assert instance.hook_type == Webhook.TYPE_UNIVERSAL

    assert instance.id in Webhook.get_all_ids()
    assert instance.id in Webhook.get_ids_by_type(instance.hook_type)


def test_update_webhook_url(db):
    instance = Webhook.create('http://www.foo.bar')
    assert instance.id > 0
    assert instance.url == 'http://www.foo.bar'
    assert instance.hook_type == Webhook.TYPE_NORMAL

    instance.update_url('http://www.foo.me')
    assert instance.url == 'http://www.foo.me'
    assert Webhook.get(instance.id).url == instance.url


def test_delete_webhook(db):
    instance = Webhook.create('http://www.foo.bar')
    assert instance.id > 0
    instance.delete()
    assert Webhook.get(instance.id) is None
    assert instance.id not in Webhook.get_all_ids()
    assert instance.id not in Webhook.get_ids_by_type(instance.hook_type)


def test_get_multi_subscriptions(db, test_webhook, test_application):
    action_type_list = [
        action_types.CREATE_CONFIG_CLUSTER,
        action_types.DELETE_CONFIG_CLUSTER
    ]
    for action_type in action_type_list:
        test_webhook.subscribe(test_application.id, action_type)
    subs = test_webhook.get_multi_subscriptions(test_application.id)
    assert len(subs) == len(action_type_list)


def test_search_subscriptions(db, test_webhook, test_application):
    action_type_list = [
        action_types.CREATE_CONFIG_CLUSTER,
        action_types.DELETE_CONFIG_CLUSTER
    ]
    for action_type in action_type_list:
        test_webhook.subscribe(test_application.id, action_type)
    subs = Webhook.search_subscriptions(application_id=test_application.id)
    assert len(subs) == len(action_type_list)


def test_webhook_subscribe(db, test_webhook, test_application):
    action_type_list = [
        action_types.CREATE_CONFIG_CLUSTER,
        action_types.DELETE_CONFIG_CLUSTER,
        action_types.CREATE_CONFIG_CLUSTER
    ]
    for action_type in action_type_list:
        test_webhook.subscribe(test_application.id, action_type)

    assert len(test_webhook.get_multi_subscriptions(test_application.id)) == \
        len(set(action_type_list))

    for action_type in action_type_list:
        sub = test_webhook.get_subscription(test_application.id, action_type)
        assert sub
        assert sub.webhook.id == test_webhook.id


def test_webhook_race_subscribe(db, test_webhook, test_application, mocker):
    action_type = action_types.CREATE_CONFIG_CLUSTER
    with mocker.patch.object(
            WebhookSubscription, 'create',
            side_effect=IntegrityError('t', 't', 't')):
        assert test_webhook.subscribe(test_application.id, action_type) is None


def test_universal_webhook_subscribe(db, test_application):
    webhook = Webhook.create('http://foo.foo.bar', Webhook.TYPE_UNIVERSAL)
    action_type = action_types.CREATE_CONFIG_CLUSTER
    sub = webhook.subscribe(test_application.id, action_type)
    assert sub is None
    assert webhook.get_subscription(test_application.id, action_type) is None


def test_webhook_unsubscribe(db, test_webhook, test_application):
    action_type = action_types.CREATE_CONFIG_CLUSTER
    test_webhook.unsubscribe(test_application.id, action_type)
    test_webhook.subscribe(test_application.id, action_type)
    assert test_webhook.get_subscription(test_application.id, action_type)

    test_webhook.unsubscribe(test_application.id, action_type)
    assert test_webhook.get_subscription(
        test_application.id, action_type) is None


def test_batch_unsubscribe(db, test_webhook, test_application):
    action_type_list = [
        action_types.CREATE_CONFIG_CLUSTER,
        action_types.DELETE_CONFIG_CLUSTER
    ]
    for action_type in action_type_list:
        test_webhook.subscribe(test_application.id, action_type)

    test_webhook.batch_unsubscribe(test_application.id)
    subs = test_webhook.get_multi_subscriptions(test_application.id)
    assert len(subs) == 0


def test_get_all_universal(db):
    Webhook.create('http://uni.foo.bar', hook_type=Webhook.TYPE_UNIVERSAL)
    Webhook.create('http://normal.foo.bar')
    hooks = Webhook.get_all_universal()
    assert [x.url for x in hooks] == ['http://uni.foo.bar']


def test_delete_webhook_subscription(db, test_webhook, test_application):
    action_type = action_types.CREATE_CONFIG_CLUSTER
    sub = test_webhook.subscribe(test_application.id, action_type)
    assert sub.id == WebhookSubscription.get_id(
        test_application.id, test_webhook.id, action_type)
    assert sub.id in WebhookSubscription.get_ids(
        test_application.id, test_webhook.id, action_type)

    sub.delete()
    assert WebhookSubscription.get_id(
        test_application.id, test_webhook.id, action_type) is None
    assert sub.id not in WebhookSubscription.get_ids(
        test_application.id, test_webhook.id, action_type)
