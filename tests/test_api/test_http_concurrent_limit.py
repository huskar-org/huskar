from __future__ import absolute_import

from copy import deepcopy
import json

import gevent
from gevent.queue import Queue, Empty
from flask import abort
from pytest import fixture, raises, mark

from huskar_api import settings
from huskar_api.api.utils import login_required
from huskar_api.app import create_app
from huskar_api.ext import sentry
from huskar_api.models import redis_client
from huskar_api.switch import switch, SWITCH_ENABLE_CONCURRENT_LIMITER


@fixture
def app(mocker):
    mocker.patch.object(sentry, 'client', None)
    mocker.patch.object(settings, 'SENTRY_DSN',
                        'gevent+http://foo:bar@example.com/1')

    app = create_app()
    app.config['PROPAGATE_EXCEPTIONS'] = False

    @app.route('/api/internal_server_error')
    def internal_server_error():
        raise Exception('meow')

    @app.route('/api/busy')
    def busy():
        gevent.sleep(1)
        return 'busy'

    @app.route('/api/busy_with_login')
    @login_required
    def busy_with_login():
        gevent.sleep(1)
        return 'busy'

    @app.route('/api/busy_with_bad_request')
    def busy_with_bad_request():
        gevent.sleep(1)
        abort(400, 'bad request')

    return app


@fixture(scope='function', autouse=True)
def clear_redis(redis_client, redis_flushall):
    try:
        redis_flushall(redis_client)
        yield
    finally:
        redis_flushall(redis_client)


@mark.parametrize('switch_on', [True, False])
@mark.parametrize('url', ['/api/busy', '/api/busy_with_bad_request'])
def test_default_anonymous_no_concurrent_limit(
        client, client_ip, switch_on, mocker, url):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_CONCURRENT_LIMITER:
            return switch_on
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    if not switch_on:
        mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', {
            '__default__': {
                'ttl': 100,
                'capacity': 1,
            }
        })

    def worker(queue):
        response = client.get(url)
        if response.status_code == 429:
            queue.put(429)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    with raises(Empty):
        queue.get_nowait()


@mark.parametrize('switch_on', [True, False])
def test_default_logged_no_concurrent_limit(
        client, client_ip, test_token, switch_on, mocker):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_CONCURRENT_LIMITER:
            return switch_on
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    if not switch_on:
        mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', {
            '__default__': {
                'ttl': 100,
                'capacity': 1,
            }
        })

    def worker(queue):
        response = client.get(
            '/api/busy_with_login', headers={
                'Authorization': test_token,
            })
        if response.status_code == 429:
            queue.put(429)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    with raises(Empty):
        queue.get_nowait()


@mark.xparametrize
@mark.parametrize('url', ['/api/busy', '/api/busy_with_bad_request'])
def test_anonymous_with_concurrent_limit(
        client, client_ip, mocker, configs, url):
    cfg = deepcopy(configs[0])
    if '127.0.0.1' in cfg:
        cfg[client_ip] = cfg.pop('127.0.0.1')
    mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', cfg)

    def worker(queue):
        response = client.get(url)
        if response.status_code == 429:
            queue.put(429)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    assert queue.get_nowait() == 429


@mark.xparametrize
def test_login_with_concurrent_limit(
        client, client_ip, mocker, test_user, test_token,
        configs, use_username):
    if '127.0.0.1' in configs:
        configs[client_ip] = configs.pop('127.0.0.1')
    if use_username:
        configs.update({
            test_user.username: {
                'ttl': 100,
                'capacity': 1,
            }
        })
    mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', configs)

    def worker(queue):
        response = client.get('/api/busy_with_login', headers={
            'Authorization': test_token,
        })
        if response.status_code != 200:
            queue.put(response.status_code)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    assert queue.get_nowait() == 429


@mark.parametrize('url', ['/api/busy', '/api/busy_with_login'])
def test_anonymous_no_concurrent_limit_because_remain_count(
        client, client_ip, mocker, url):
    mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', {
        '__anonymous__': {
            'ttl': 100,
            'capacity': 100,
        }
    })

    def worker(queue):
        response = client.get(url)
        if response.status_code == 429:
            queue.put(429)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    with raises(Empty):
        queue.get_nowait()


def test_logged_no_concurrent_limit_because_remain_count(
        client, client_ip, test_user, test_token, mocker):
    mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', {
        test_user.username: {
            'ttl': 100,
            'capacity': 100,
        }
    })

    def worker(queue):
        response = client.get(
            '/api/busy_with_login', headers={
                'Authorization': test_token,
            })
        if response.status_code == 429:
            queue.put(429)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    with raises(Empty):
        queue.get_nowait()


@mark.parametrize('error_method,cause_limit', [
    ('eval', False),
    ('zrem', True)
])
def test_logged_concurrent_limit_with_redis_error(
        client, client_ip, test_user, test_token, mocker,
        error_method, cause_limit):
    mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', {
        test_user.username: {
            'ttl': 100,
            'capacity': 1,
        }
    })
    mocker.patch.object(redis_client, error_method, side_effect=Exception)

    def worker(queue):
        response = client.get(
            '/api/busy_with_login', headers={
                'Authorization': test_token,
            })
        if response.status_code == 429:
            queue.put(429)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    if cause_limit:
        assert queue.get_nowait() == 429
    else:
        with raises(Empty):
            queue.get_nowait()


def test_long_polling_no_end_with_concurrent_limit(
        client, client_ip, mocker, test_application, test_application_token):
    mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', {
        '__default__': {
            'ttl': 100,
            'capacity': 1,
        }
    })
    data = {
        'service': {
            test_application.application_name: ['foo'],
        },
    }
    result = []

    for _ in range(3):
        response = client.post(
            '/api/data/long-polling', headers={
                'Authorization': test_application_token,
            }, content_type='application/json', data=json.dumps(data))
        result.append(response.status_code)
    assert 429 in result


def test_long_polling_end_no_concurrent_limit(
        client, client_ip, mocker, test_application, test_application_token):
    mocker.patch.object(settings, 'CONCURRENT_LIMITER_SETTINGS', {
        '__default__': {
            'ttl': 100,
            'capacity': 1,
        }
    })
    data = {
        'service': {
            test_application.application_name: ['foo'],
        },
    }
    result = []

    for _ in range(3):
        response = client.post(
            '/api/data/long-polling', headers={
                'Authorization': test_application_token,
            }, query_string={'life_span': 1},
            content_type='application/json', data=json.dumps(data))
        with gevent.Timeout(2):
            list(response.response)
        result.append(response.status_code)
    assert 429 not in result


def test_update_settings():
    config = {
        '__default__': {
            'ttl': 1,
            'capacity': 233,
        },
        'foo.bar': {
            'ttl': 233,
            'capacity': 666,
        },
    }
    try:
        assert settings.CONCURRENT_LIMITER_SETTINGS == {}
        settings.update_concurrent_limiter_settings(config)
        assert settings.CONCURRENT_LIMITER_SETTINGS == config
    finally:
        settings.update_concurrent_limiter_settings({})
