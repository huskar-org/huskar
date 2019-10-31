from __future__ import absolute_import

import json

from kazoo.exceptions import NoNodeError
from pytest import fixture, raises

from huskar_api.models.exceptions import OutOfSyncError
from huskar_api.models.instance.schema import Instance
from ..utils import assert_response_ok


@fixture
def blacklist(zk):
    path = '/huskar/config/arch.huskar_api/overall/AUTH_IP_BLACKLIST'

    def update(data):
        zk.ensure_path(path)
        zk.set(path, json.dumps(data))
        return data

    def reset():
        zk.ensure_path(path)
        zk.delete(path)

    def get():
        return json.loads(zk.get(path)[0])

    update.reset = reset
    update.get = get

    return update


def test_get_blacklist(client, admin_token, minimal_mode, blacklist):
    blacklist.reset()

    r = client.get(
        '/api/_internal/ops/blacklist', headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert r.json['data'] == {'blacklist': []}

    blacklist(['169.254.0.12'])

    r = client.get(
        '/api/_internal/ops/blacklist', headers={'Authorization': admin_token})
    assert_response_ok(r)
    assert r.json['data'] == {'blacklist': ['169.254.0.12']}


def test_add_blacklist(client, admin_token, minimal_mode, blacklist):
    blacklist.reset()

    with raises(NoNodeError):
        blacklist.get()

    r = client.post(
        '/api/_internal/ops/blacklist', data={'remote_addr': '169.254.1.1'},
        headers={'Authorization': admin_token})
    assert_response_ok(r)

    assert blacklist.get() == ['169.254.1.1']

    r = client.post(
        '/api/_internal/ops/blacklist', data={'remote_addr': '169.254.1.2'},
        headers={'Authorization': admin_token})
    assert_response_ok(r)

    assert blacklist.get() == ['169.254.1.1', '169.254.1.2']

    r = client.post(
        '/api/_internal/ops/blacklist', data={'remote_addr': '169.254.1'},
        headers={'Authorization': admin_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'remote_addr is invalid'

    assert blacklist.get() == ['169.254.1.1', '169.254.1.2']


def test_add_blacklist_oos(mocker, client, admin_token, minimal_mode):
    mocker.patch.object(Instance, 'save', side_effect=OutOfSyncError())

    r = client.post(
        '/api/_internal/ops/blacklist', data={'remote_addr': '169.254.1.1'},
        headers={'Authorization': admin_token})
    assert r.status_code == 409, r.data
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == 'resource is modified by another request'


def test_delete_blacklist(client, admin_token, minimal_mode, blacklist):
    blacklist.reset()
    blacklist(['169.254.1.1', '169.254.1.2'])

    assert blacklist.get() == ['169.254.1.1', '169.254.1.2']

    r = client.delete(
        '/api/_internal/ops/blacklist', data={'remote_addr': '169.254.1'},
        headers={'Authorization': admin_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'BadRequest'
    assert r.json['message'] == 'remote_addr is invalid'

    assert blacklist.get() == ['169.254.1.1', '169.254.1.2']

    r = client.delete(
        '/api/_internal/ops/blacklist', data={'remote_addr': '169.254.1.1'},
        headers={'Authorization': admin_token})
    assert_response_ok(r)

    assert blacklist.get() == ['169.254.1.2']

    r = client.delete(
        '/api/_internal/ops/blacklist', data={'remote_addr': '169.254.1.2'},
        headers={'Authorization': admin_token})
    assert_response_ok(r)

    assert blacklist.get() == []
