from __future__ import absolute_import

import datetime

from pytest import fixture
from flask import g
from freezegun import freeze_time

from huskar_api.app import create_app


@fixture
def app():
    app = create_app()
    app.config['PROPAGATE_EXCEPTIONS'] = False

    @app.route('/api/minimal-mode')
    def minimal_mode():
        return unicode(g.auth.is_minimal_mode)

    return app


def test_graceful_startup(mocker, client):
    mocker.patch('huskar_api.settings.MM_GRACEFUL_STARTUP_TIME', 600)

    r = client.get('/api/minimal-mode')
    assert r.headers['X-Minimal-Mode'] == '1'
    assert r.headers['X-Minimal-Mode-Reason'] == 'startup'
    assert r.data == u'True'

    with freeze_time() as frozen_time:
        frozen_time.tick(delta=datetime.timedelta(seconds=600))
        r = client.get('/api/minimal-mode')
        assert r.headers.get('X-Minimal-Mode') is None
        assert r.headers.get('X-Minimal-Mode-Reason') is None
        assert r.data == u'False'
