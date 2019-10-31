from __future__ import absolute_import

from copy import deepcopy

import gevent
from gevent.queue import Queue, Empty
from flask import abort
from pytest import fixture, raises, mark

from huskar_api import settings
from huskar_api.api.utils import login_required
from huskar_api.app import create_app
from huskar_api.ext import sentry
from huskar_api.models import redis_client
from huskar_api.switch import switch, SWITCH_ENABLE_RATE_LIMITER


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

    @app.route('/api/bad_method', methods=['POST'])
    def bad_method():
        abort(400, 'woo')

    @app.route('/api/fine')
    def fine():
        return 'fine'

    @app.route('/api/need_login')
    @login_required
    def need_login():
        return 'fine'

    return app


@fixture(scope='function', autouse=True)
def clear_redis(redis_client, redis_flushall):
    try:
        redis_flushall(redis_client)
        yield
    finally:
        redis_flushall(redis_client)


@mark.parametrize('switch_on', [True, False])
@mark.parametrize('url', ['/api/fine', '/api/need_login'])
def test_default_anonymous_no_rate_limit(
        client, client_ip, switch_on, mocker, url):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_RATE_LIMITER:
            return switch_on
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    if not switch_on:
        mocker.patch.object(settings, 'RATE_LIMITER_SETTINGS', {
            '__default__': {
                'rate': 1,
                'capacity': 3,
            }
        })

    def worker(queue):
        response = client.get(url)
        if response.status_code == 429:
            queue.put(True)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    with raises(Empty):
        queue.get_nowait()


@mark.parametrize('switch_on', [True, False])
def test_default_logged_no_rate_limit(
        client, client_ip, test_token, switch_on, mocker):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_RATE_LIMITER:
            return switch_on
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    if not switch_on:
        mocker.patch.object(settings, 'RATE_LIMITER_SETTINGS', {
            '__default__': {
                'rate': 1,
                'capacity': 3,
            }
        })

    def worker(queue):
        response = client.get(
            '/api/need_login', headers={
                'Authorization': test_token,
            })
        if response.status_code == 429:
            queue.put(True)

    greenlets = []
    queue = Queue()
    for _ in range(3):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    with raises(Empty):
        queue.get_nowait()


@mark.xparametrize
@mark.parametrize('url', ['/api/fine', '/api/need_login'])
def test_anonymous_with_rate_limit(client, client_ip, mocker, configs, url):
    cfg = deepcopy(configs[0])
    if '127.0.0.1' in cfg:
        cfg[client_ip] = cfg.pop('127.0.0.1')
    mocker.patch.object(settings, 'RATE_LIMITER_SETTINGS', cfg)

    def worker(queue):
        response = client.get(url)
        if response.status_code == 429:
            queue.put(429)

    greenlets = []
    queue = Queue()
    for _ in range(5):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    assert queue.get_nowait() == 429


@mark.xparametrize
def test_logged_with_rate_limit(
        client, client_ip, mocker, test_user, test_token,
        configs, use_username):
    if '127.0.0.1' in configs:
        configs[client_ip] = configs.pop('127.0.0.1')
    if use_username:
        configs.update({
            test_user.username: {
                'rate': 1,
                'capacity': 3,
            }
        })
    mocker.patch.object(settings, 'RATE_LIMITER_SETTINGS', configs)

    def worker(queue):
        response = client.get('/api/need_login', headers={
            'Authorization': test_token,
        })
        if response.status_code == 429:
            queue.put(429)

    greenlets = []
    queue = Queue()
    for _ in range(10):
        greenlets.append(gevent.spawn(worker, queue))

    gevent.joinall(greenlets)
    assert queue.get_nowait() == 429


@mark.parametrize('url', ['/api/fine', '/api/need_login'])
def test_anonymous_no_rate_limit_because_remain_count(
        client, client_ip, mocker, url):
    mocker.patch.object(settings, 'RATE_LIMITER_SETTINGS', {
        '__anonymous__': {
            'rate': 100,
            'capacity': 300,
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


def test_logged_no_rate_limit_because_remain_count(
        client, client_ip, test_user, test_token, mocker):
    mocker.patch.object(settings, 'RATE_LIMITER_SETTINGS', {
        test_user.username: {
            'rate': 100,
            'capacity': 300,
        }
    })

    def worker(queue):
        response = client.get(
            '/api/need_login', headers={
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


@mark.parametrize('error_method', ['eval'])
def test_logged_no_rate_limit_because_redis_error(
        client, client_ip, test_user, test_token, mocker, error_method):
    mocker.patch.object(settings, 'RATE_LIMITER_SETTINGS', {
        test_user.username: {
            'rate': 1,
            'capacity': 3,
        }
    })
    mocker.patch.object(redis_client, error_method, side_effect=Exception)

    def worker(queue):
        response = client.get(
            '/api/need_login', headers={
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


def test_update_settings():
    config = {
        '__default__': {
            'rate': 1,
            'capacity': 233,
        },
        'foo.bar': {
            'rate': 233,
            'capacity': 666,
        },
    }
    try:
        assert settings.RATE_LIMITER_SETTINGS == {}
        settings.update_rate_limiter_settings(config)
        assert settings.RATE_LIMITER_SETTINGS == config
    finally:
        settings.update_rate_limiter_settings({})
