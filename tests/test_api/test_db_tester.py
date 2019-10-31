from __future__ import absolute_import

import datetime

from pytest import fixture
from flask import g
from freezegun import freeze_time
from sqlalchemy.exc import SQLAlchemyError
from redis.exceptions import RedisError

from huskar_api.app import create_app


@fixture
def app():
    app = create_app()
    app.config['PROPAGATE_EXCEPTIONS'] = False

    @app.route('/api/minimal-mode')
    def minimal_mode():
        return unicode(g.auth.is_minimal_mode)

    @app.route('/api/mysql')
    def mysql_error():
        raise SQLAlchemyError()

    @app.route('/api/redis')
    def redis_error():
        raise RedisError()

    return app


def test_minimal_mode(client):
    r = client.get('/api/minimal-mode')
    assert r.data == u'False'

    for _ in xrange(5):
        client.get('/api/mysql')
        client.get('/api/redis')

    r = client.get('/api/minimal-mode')
    assert r.headers['X-Minimal-Mode'] == '1'
    assert r.headers['X-Minimal-Mode-Reason'] == 'tester'
    assert r.data == u'True'

    with freeze_time() as frozen_time:
        for _ in xrange(10):
            client.get('/api/minimal-mode')
            frozen_time.tick(delta=datetime.timedelta(seconds=120))
        r = client.get('/api/minimal-mode')
        assert r.headers.get('X-Minimal-Mode') is None
        assert r.headers.get('X-Minimal-Mode-Reason') is None
        assert r.data == u'False'
