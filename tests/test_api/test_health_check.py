from __future__ import absolute_import

from ..utils import assert_response_ok


def test_ok(client):
    r = client.get('/api/health_check')
    assert_response_ok(r)
    assert r.json['data'] == 'ok'
