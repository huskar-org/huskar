from __future__ import absolute_import

import json

from pytest import mark

from huskar_api.models.audit import action_types
from huskar_api.models.webhook import Webhook
from ..utils import assert_response_ok


def add_test_webhook_sub(prepare_data, application_id):
    for data in prepare_data:
        webhook = Webhook.create(
            data['webhook_url'], data.get('webhook_type', 0))
        if webhook.is_normal:
            for action_name in data['event_list']:
                action_type = getattr(action_types, action_name)
                webhook.subscribe(application_id, action_type)
        yield webhook, data


@mark.xparametrize
def test_add_webhook_subscriptions(
        db, client, test_application, present_data, input_data, admin_token):
    add_test_webhook_sub(present_data, test_application.id)
    headers = {
        'Authorization': admin_token,
        'Content-Type': 'application/json'
    }
    query_string = {
        'webhook_type': input_data['webhook_type'],
        'application_name': test_application.application_name
    }
    r = client.post('/api/webhook', data=json.dumps(input_data),
                    headers=headers, query_string=query_string)
    assert_response_ok(r)
    assert r.json['data'] is None

    if int(input_data['webhook_type']) == 1:
        event_num = 0
    else:
        event_num = len(input_data['event_list'])

    webhook_sub_set = Webhook.search_subscriptions(
        application_id=test_application.id)
    assert len(webhook_sub_set) == event_num


def test_add_webhook_with_invalid_authority(
        client, test_application_token):
    data = json.dumps({
        'webhook_url': 'http://www.foo.bar',
        'event_list': ['CREATE_CONFIG_CLUSTER']
    })
    headers = {
        'Authorization': test_application_token,
        'Content-Type': 'application/json'
    }
    query_string = {
        'webhook_type': 0,
        'application_name': 'foo'
    }
    r = client.post('/api/webhook', data=data,
                    headers=headers, query_string=query_string)
    assert r.status_code == 400
    assert r.json['status'] == 'ApplicationNotExistedError'
    assert r.json['message'] == "application: foo doesn't exist"

    query_string.update(webhook_type=1)
    r = client.post('/api/webhook', data=data,
                    headers=headers, query_string=query_string)
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert r.json['message'] == 'only admin can add universal webhook'


@mark.xparametrize
def test_add_webhook_subscriptions_with_invalid_arguments(
        client, test_application, test_application_token, input_data,
        expected_result):
    headers = {
        'Authorization': test_application_token,
        'Content-Type': 'application/json'
    }
    query_string = {
        'webhook_type': 0,
        'application_name': test_application.application_name
    }
    r = client.post('/api/webhook', data=json.dumps(input_data),
                    headers=headers, query_string=query_string)
    assert r.status_code == expected_result['status_code']
    assert r.json['status'] == expected_result['status']
    assert r.json['message'] == expected_result['message']


@mark.xparametrize
def test_update_webhook_subscriptions(
        db, client, test_application, test_application_token,
        admin_token, present_data, input_data):
    webhooks = add_test_webhook_sub(present_data, test_application.id)
    webhook = list(webhooks)[0][0]
    if webhook.is_normal:
        token = test_application_token
        event_num = len(input_data['event_list'])
    else:
        token = admin_token
        event_num = 0
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json'
    }
    query_string = {'application_name': test_application.application_name}

    r = client.put('/api/webhook/%s' % webhook.id, data=json.dumps(input_data),
                   headers=headers, query_string=query_string)
    assert_response_ok(r)
    assert r.json['data'] is None

    webhook_subs = Webhook.search_subscriptions(
        application_id=test_application.id)
    assert len(webhook_subs) == event_num


@mark.xparametrize
def test_delete_webhook(
        db, client, test_application, test_application_token,
        admin_token, present_data, input_data):
    webhooks = add_test_webhook_sub(present_data, test_application.id)
    webhook = list(webhooks)[0][0]
    if webhook.is_normal:
        token = test_application_token
    else:
        token = admin_token
    headers = {'Authorization': token}
    query_string = {'application_name': test_application.application_name}
    r = client.delete('/api/webhook/%s' % webhook.id, headers=headers,
                      query_string=query_string)
    assert_response_ok(r)
    assert r.json['data'] is None

    assert Webhook.get(webhook.id) is None
    assert len(Webhook.search_subscriptions(webhook_id=webhook.id)) == 0


def test_delete_webhook_fialed(
        client, test_application, test_application_token):
    headers = {'Authorization': test_application_token}
    r = client.get('/api/webhook/100', headers=headers)
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'Webhook not registered.'

    webhook = Webhook.create('http://www.foo.bar')
    r = client.delete('/api/webhook/%s' % webhook.id, headers=headers,
                      query_string={'application_name': 'foo'})
    assert r.status_code == 400


@mark.xparametrize
def test_get_webhook_subscriptions_with_application(
        client, test_application, test_application_token,
        present_data):
    expected_data = []
    webhook_subs = add_test_webhook_sub(present_data[0], test_application.id)
    for (webhook, data) in webhook_subs:
        expected_data.append({
            'webhook_id': webhook.id,
            'webhook_url': webhook.url,
            'webhook_type': webhook.hook_type,
            'event_list': data['event_list']
        })

    headers = {'Authorization': test_application_token}
    r = client.get(
        '/api/webhook/application/%s' % test_application.application_name,
        headers=headers)
    assert_response_ok(r)
    assert r.json['data']['webhook_list'] == expected_data


def test_get_webhook_subscriptions_with_application_failed(
        client, test_application, test_application_token):
    headers = {'Authorization': test_application_token}
    r = client.get('/api/webhook/application/foo', headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'ApplicationNotExistedError'
    assert r.json['message'] == "application: foo doesn't exist"


def test_get_webhook_list(client, test_application_token):
    webhook_list = [
        {
            'webhook_url': 'http://www.foo.bar',
            'webhook_type': 0,
        },
        {
            'webhook_url': 'http://www.foo.me',
            'webhook_type': 1,
        },
        {
            'webhook_url': 'http://www.bar.me',
            'webhook_type': 0
        }
    ]
    for data in webhook_list:
        webhook = Webhook.create(data['webhook_url'], data['webhook_type'])
        data.update(webhook_id=webhook.id)

    headers = {'Authorization': test_application_token}
    r = client.get('/api/webhook', headers=headers)
    assert_response_ok(r)
    assert r.json['data']['webhook_list'] == webhook_list


def test_get_webhook(client, test_application_token, test_application):
    webhook_normal = Webhook.create('http://www.foo.bar')
    headers = {'Authorization': test_application_token}

    query_string = {'application_name': test_application.application_name}
    r = client.get('/api/webhook/%s' % webhook_normal.id, headers=headers,
                   query_string=query_string)
    assert_response_ok(r)
    assert r.json['data'] == {
        'webhook_id': webhook_normal.id,
        'webhook_url': webhook_normal.url,
        'webhook_type': webhook_normal.hook_type,
        'event_list': []
    }

    webhook_universal = Webhook.create('http://www.foo.bar', 1)
    headers = {'Authorization': test_application_token}
    r = client.get('/api/webhook/%s' % webhook_universal.id, headers=headers,
                   query_string=query_string)
    assert_response_ok(r)
    assert r.json['data'] == {
        'webhook_id': webhook_universal.id,
        'webhook_url': webhook_universal.url,
        'webhook_type': webhook_universal.hook_type,
        'event_list': []
    }


def test_update_webhook_then_get(
        client, test_application_token, test_application):
    webhook_normal = Webhook.create('http://www.foo.bar')
    headers = {
        'Authorization': test_application_token,
        'Content-Type': 'application/json',
    }

    query_string = {'application_name': test_application.application_name}
    r = client.get('/api/webhook/%s' % webhook_normal.id, headers=headers,
                   query_string=query_string)
    assert_response_ok(r)
    assert r.json['data'] == {
        'webhook_id': webhook_normal.id,
        'webhook_url': webhook_normal.url,
        'webhook_type': webhook_normal.hook_type,
        'event_list': []
    }

    payload = {
        'event_list': ['UPDATE_CONFIG', 'DELETE_CONFIG'],
        'webhook_url': 'http://abc.example.com',
        'webhook_type': 0,
    }
    r = client.put('/api/webhook/%s' % webhook_normal.id, headers=headers,
                   query_string=query_string, data=json.dumps(payload))
    assert_response_ok(r)

    r = client.get('/api/webhook/%s' % webhook_normal.id, headers=headers,
                   query_string=query_string)
    assert_response_ok(r)
    assert r.json['data'] == {
        'webhook_id': webhook_normal.id,
        'webhook_url': payload['webhook_url'],
        'webhook_type': webhook_normal.hook_type,
        'event_list': payload['event_list'],
    }
