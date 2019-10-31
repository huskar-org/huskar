from __future__ import absolute_import

import itertools
import json
import logging

from pytest import fixture, mark

from huskar_api.app import create_app
from huskar_api.api.utils import api_response
from huskar_api.models.instance import InfraInfo
from ..utils import assert_response_ok


@fixture
def app():
    app = create_app()
    app.config['PROPAGATE_EXCEPTIONS'] = False

    @app.route('/api/this_is_bad')
    def this_is_bad():
        raise Exception('meow')

    @app.route('/api/this_is_okay')
    def this_is_okay():
        return api_response()

    @app.route('/api/this_is_sad')
    def this_is_sad():
        return 'sad', 400

    @app.route('/api/post_list', methods=['POST'])
    def post_list():
        return api_response()

    return app


@fixture
def log(mocker):
    logger = logging.getLogger('huskar_api.api.middlewares.logger')
    return mocker.patch.object(logger, 'info', autospec=True)


def test_okay(client, log):
    r = client.get('/api/this_is_okay?a=1', data={'b': '2'})
    assert r.status_code == 200
    assert len(log.mock_calls) == 1
    _, args, _ = log.mock_calls[0]
    assert args[1:-6] == (
        'Ok', 'anonymous_user 127.0.0.1', 'GET', '/api/this_is_okay')
    assert json.loads(args[-6]) == {'a': '1', 'b': '2'}
    assert args[-4] == 'unknown'  # soa_mode
    assert args[-3] == 'unknown'  # cluster_name
    assert args[-2] == 'SUCCESS'  # status
    assert args[-1] == 200


def test_okay_with_json(client, log):
    r = client.get('/api/this_is_okay?a=1', data=json.dumps({'b': '2'}),
                   headers={'content-type': 'application/json'})
    assert r.status_code == 200
    assert len(log.mock_calls) == 1
    _, args, kwargs = log.mock_calls[0]
    assert json.loads(args[-6]) == {'a': '1', 'b': '2'}


def test_okay_with_post_list_json(client, log):
    data = [{'b': '2', 'c': 3}]
    r = client.post('/api/post_list', data=json.dumps(data),
                    headers={'content-type': 'application/json'})
    assert r.status_code == 200
    assert len(log.mock_calls) == 1
    _, args, kwargs = log.mock_calls[0]
    assert args[-6] == '{}'


def test_okay_with_soa_mode(client, log):
    r = client.get('/api/this_is_okay?a=1', headers={'X-SOA-Mode': 'prefix'})
    assert r.status_code == 200
    assert len(log.mock_calls) == 1
    _, args, _ = log.mock_calls[0]
    assert args[-4] == 'prefix'


def test_okay_with_cluster_name(client, log):
    r = client.get('/api/this_is_okay?a=1', headers={'X-Cluster-Name': 'cn'})
    assert r.status_code == 200
    assert len(log.mock_calls) == 1
    _, args, _ = log.mock_calls[0]
    assert args[-3] == 'cn'


def test_failed(client, log):
    r = client.get('/api/this_is_sad')
    assert r.status_code == 400
    assert len(log.mock_calls) == 1
    _, args, _ = log.mock_calls[0]
    assert args[1:-5] == (
        'Failed', 'anonymous_user 127.0.0.1', 'GET', '/api/this_is_sad', '{}')
    assert args[-2] == 'unknown'
    assert args[-1] == 400


def test_crashed(client, log):
    r = client.get('/api/this_is_bad')
    assert r.status_code == 500
    assert len(log.mock_calls) == 1
    _, args, _ = log.mock_calls[0]
    assert args[1:-5] == (
        'Failed', 'anonymous_user 127.0.0.1', 'GET', '/api/this_is_bad', '{}')
    assert args[-2] == 'InternalServerError'
    assert args[-1] == 500


@mark.parametrize('key', [
    'password',
    'old_password',
    'new_password',
    'value',
])
def test_with_sensitive_data(client, key, log):
    data = {
        key: '233',
        'foo': 'bar',
    }
    for k in itertools.chain(*InfraInfo._INFRA_CONFIG_URL_ATTRS.values()):
        data[k] = '666'

    r = client.post('/api/post_list', data=data)
    assert r.status_code == 200
    _, args, _ = log.mock_calls[0]
    request_args = json.loads(args[-6])
    data.pop(key)

    assert data == request_args


@mark.parametrize('url', ['/api/health_check'])
def test_skiped(client, log, url):
    r = client.get(url)
    assert_response_ok(r)
    assert not log.called
