from __future__ import absolute_import

from pytest import fixture, mark

from huskar_api import settings
from huskar_api.switch import SWITCH_ENABLE_CONFIG_AND_SWITCH_WRITE
from ..utils import assert_response_ok


@fixture
def test_application_name(test_application):
    return test_application.application_name


def test_update_config_and_switch_readonly_whitelist():
    settings.update_config_and_switch_readonly_whitelist(['foo.test'])
    assert settings.CONFIG_AND_SWITCH_READONLY_WHITELIST == \
        frozenset(['foo.test', 'arch.huskar_api'])

    settings.update_config_and_switch_readonly_whitelist(['arch.huskar_api'])
    assert settings.CONFIG_AND_SWITCH_READONLY_WHITELIST == \
        frozenset(['arch.huskar_api'])


def test_update_config_and_switch_readonly_blacklist():
    settings.update_config_and_switch_readonly_blacklist(['foo.test'])
    assert settings.CONFIG_AND_SWITCH_READONLY_BLACKLIST == \
        frozenset(['foo.test'])


@mark.xparametrize
def test_config_and_switch_readonly_middleware(
        client, mock_switches, test_application_name,
        test_application_token, _path, _method, _read_only,
        _data, _status, _in_whitelist, _in_blacklist):
    mock_switches({SWITCH_ENABLE_CONFIG_AND_SWITCH_WRITE: _read_only})
    headers = {'Authorization': test_application_token}

    settings.update_config_and_switch_readonly_whitelist([])
    if _in_whitelist:
        settings.update_config_and_switch_readonly_whitelist(
            [test_application_name])

    settings.update_config_and_switch_readonly_blacklist([])
    if _in_blacklist:
        settings.update_config_and_switch_readonly_blacklist(
            [test_application_name])

    r = client.open(
        method=_method, path=_path % test_application_name,
        data=_data, headers=headers,
    )

    if _status:
        assert_response_ok(r)
    else:
        assert r.status_code == 403
