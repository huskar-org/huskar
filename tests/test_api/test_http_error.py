from __future__ import absolute_import

from pytest import fixture, mark
from flask import request, abort

from huskar_api import settings
from huskar_api.app import create_app
from huskar_api.ext import sentry


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

    @app.route('/api/bad_key')
    def bad_key():
        return request.args['you_bad_bad']

    @app.route('/api/bad_method', methods=['POST'])
    def bad_method():
        abort(400, 'woo')

    @app.route('/api/fine')
    def fine():
        return 'fine'

    return app


def test_400_bad_key(client):
    r = client.get('/api/bad_key')
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == '"you_bad_bad" is required field.'
    assert r.json['data'] is None


def test_400_abort(client):
    r = client.post('/api/bad_method')
    assert r.status_code == 400
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'woo'
    assert r.json['data'] is None


def test_401(client):
    r = client.post('/api/application')
    assert r.status_code == 401
    assert r.json['status'] == 'Unauthorized'
    assert r.json['message'] == 'The token is missing, invalid or expired.'
    assert r.json['data'] is None


def test_404(client):
    r = client.get('/api/foo/bar')
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'].startswith(
        'The requested URL was not found on the server')
    assert r.json['data'] is None


def test_405(client):
    r = client.get('/api/bad_method')
    assert r.status_code == 405
    assert r.json['status'] == 'MethodNotAllowed'
    assert r.json['message']
    assert r.json['data'] is None


def test_500(mocker, app, client):
    mocker.spy(app, 'log_exception')
    mocker.spy(sentry, 'captureException')

    assert app.log_exception.call_count == 0
    assert sentry.captureException.call_count == 0

    r = client.get('/api/internal_server_error')
    assert r.status_code == 500
    assert r.json['status'] == 'InternalServerError'
    assert r.json['message'].startswith(
        'The server encountered an internal error')

    assert app.log_exception.call_count == 1
    assert sentry.captureException.call_count == 1


@mark.parametrize('is_testing,status_code', [(True, 500), (False, 200)])
def test_db_close(mocker, db, client, is_testing, status_code):
    mocker.patch('huskar_api.models.DBSession.remove', side_effect=ValueError)
    mocker.patch('huskar_api.settings.TESTING', is_testing)
    logger = mocker.patch(
        'huskar_api.api.middlewares.db.logger', autospec=True)
    logger.exception.assert_not_called()
    r = client.get('/api/fine')
    assert r.status_code == status_code
    logger.exception.assert_called()


def test_blacklist(mocker, client):
    remote_addr = '169.254.134.133'

    def notify(value):
        watchers = settings.config_manager.external_watchers
        for watcher in watchers['AUTH_IP_BLACKLIST']:
            watcher(value)

    def request():
        environ = {'REMOTE_ADDR': remote_addr}
        return client.get('/api/fine', environ_base=environ)

    r = request()
    assert r.status_code == 200

    notify(['169.254.0.1', remote_addr])

    r = request()
    assert r.status_code == 403
    assert r.json['status'] == 'Forbidden'
    assert r.json['data'] is None
    assert r.json['message'] == 'The IP address is blacklisted'

    notify(['169.254.0.1'])

    r = request()
    assert r.status_code == 200
