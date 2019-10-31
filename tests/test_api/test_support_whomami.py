from __future__ import absolute_import

from ..utils import assert_response_ok


def test_whoami(
        client, test_application, test_application_token,
        test_user, admin_user, test_token, admin_token):
    r = client.get('/api/whoami', headers={
        'Authorization': test_application_token})
    assert_response_ok(r)
    assert r.json['data'] == {
        'is_anonymous': False,
        'is_application': True,
        'is_minimal_mode': False,
        'is_admin': False,
        'username': test_application.application_name,
    }

    r = client.get('/api/whoami', headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.json['data'] == {
        'is_anonymous': False,
        'is_application': False,
        'is_minimal_mode': False,
        'is_admin': False,
        'username': test_user.username,
    }

    r = client.get('/api/whoami', headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert r.json['data'] == {
        'is_anonymous': False,
        'is_application': False,
        'is_minimal_mode': False,
        'is_admin': True,
        'username': admin_user.username,
    }

    r = client.get('/api/whoami')
    assert_response_ok(r)
    assert r.json['data'] == {
        'is_anonymous': True,
        'is_application': False,
        'is_minimal_mode': False,
        'is_admin': False,
        'username': '',
    }
