from __future__ import absolute_import

from hashlib import md5

from flask import request
from pytest import fixture, raises, mark

from huskar_api import settings
from huskar_api.app import create_app
from huskar_api.api.utils import (
    api_response, with_etag, deliver_email_safe, with_cache_control)
from huskar_api.extras.email import EmailTemplate, EmailDeliveryError
from huskar_api.service.exc import DuplicatedEZonePrefixError
from huskar_api.service.utils import (
    check_cluster_name, check_cluster_name_in_creation)
from ..utils import assert_response_ok


@fixture
def app():
    app = create_app()
    app.config['PROPAGATE_EXCEPTIONS'] = False

    @app.route('/api/test_etag')
    @with_etag
    def test_etag():
        return api_response(data={
            '233': request.args.get('value', '666'),
        })

    @app.route('/api/test_cache_control')
    @with_cache_control
    def test_cache_control():
        return api_response()

    @app.route('/api/test_email')
    def test_email():
        deliver_email_safe(EmailTemplate.DEBUG, 't@example.com', {'foo': 't'})
        return api_response()

    return app


def test_etag(client):
    url = '/api/test_etag'
    r = client.get(url)
    assert_response_ok(r)
    etag = '"{0}"'.format(md5(r.data).hexdigest())
    assert r.headers['ETag'] == etag

    r = client.get(url, headers={'If-None-Match': etag}, buffered=True)
    assert r.status_code == 304
    assert r.headers['ETag'] == etag
    assert r.data == ''

    url = '/api/test_etag?value=233'
    r = client.get(url, headers={'If-None-Match': etag}, buffered=True)
    assert_response_ok(r)
    assert r.headers['ETag'] != etag
    assert r.data != ''


def test_cache_control(client, mocker):
    url = '/api/test_cache_control'
    r = client.get(url)
    assert_response_ok(r)
    assert 'Cache-Control' not in r.headers

    mocker.patch.object(settings, 'CACHE_CONTROL_SETTINGS', {
        'test_cache_control': {'max_age': 3, 'public': True},
    })
    r = client.get(url)
    assert_response_ok(r)
    assert set(r.headers.get('Cache-Control').split(', ')
               ) == {'max-age=3', 'public'}


def test_email(client, mocker):
    deliver_email = mocker.patch('huskar_api.api.utils.deliver_email')
    deliver_email.side_effect = [None, EmailDeliveryError()]

    logger = mocker.patch('huskar_api.api.utils.logger', autospec=True)

    r = client.get('/api/test_email')
    assert_response_ok(r)
    logger.exception.assert_not_called()

    r = client.get('/api/test_email')
    assert_response_ok(r)
    logger.exception.assert_called_once()


@mark.parametrize('cluster_name,valid', [
    ('stable', True),
    ('stable-altb1', True),
    ('stable-altb1-stable', True),
    ('altb1', True),
    ('altb1-stable', True),
    ('altb1-altb1-stable', False),
    ('altb1-altb1-altb1-stable', False),
    ('altb1-alta1-stable', True),
    ('altb1-altb1-alta1-stable', False),
    ('altb1-alta1-alta1-stable', True),
])
def test_check_cluster_name(mocker, cluster_name, valid):
    mocker.patch('huskar_api.settings.ROUTE_EZONE_LIST', ['altb1', 'alta1'])

    if not valid:
        with raises(DuplicatedEZonePrefixError):
            check_cluster_name(cluster_name)
    else:
        assert check_cluster_name(cluster_name)


@mark.parametrize('cluster_name,valid', [
    ('stable', True),
    ('stable-altb1', True),
    ('stable-altb1-stable', True),
    ('altb1', True),
    ('altb1-stable', True),
    ('altb1-altb1-stable', False),
    ('altb1-altb1-altb1-stable', False),
    ('altb1-alta1-stable', True),
    ('altb1-altb1-alta1-stable', False),
    ('altb1-alta1-alta1-stable', True),
])
def test_check_cluster_name_in_creation(mocker, cluster_name, valid):
    mocker.patch('huskar_api.settings.ROUTE_EZONE_LIST', ['altb1', 'alta1'])

    if not valid:
        with raises(DuplicatedEZonePrefixError):
            check_cluster_name_in_creation(cluster_name)
    else:
        assert check_cluster_name_in_creation(cluster_name)
