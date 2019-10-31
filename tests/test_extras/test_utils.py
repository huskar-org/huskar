from __future__ import absolute_import

import pytest
import requests

from sqlalchemy.exc import OperationalError

from huskar_api.switch import (
    SWITCH_ENABLE_AUDIT_LOG, SWITCH_ENABLE_MINIMAL_MODE)
from huskar_api.models.audit import action_types
from huskar_api.extras.utils import trace_remote_http_call, huskar_audit_log


def test_trace_remote_http_call_200(req_mocker, mocker):
    req_mocker.get('/test', status_code=200)
    monitor = mocker.patch('huskar_api.extras.utils.monitor_client')
    url = 'http://example.com/test'
    with trace_remote_http_call(url):
        requests.get(url)

    timing = monitor.timing
    assert timing.call_count == 1
    assert timing.call_args[0][0] == 'remote_http_call.timer'


def test_trace_remote_http_call_403(req_mocker, mocker):
    req_mocker.get('/test', status_code=400)
    monitor = mocker.patch('huskar_api.extras.utils.monitor_client')
    url = 'http://example.com/test'
    with pytest.raises(requests.HTTPError):
        with trace_remote_http_call(url):
            r = requests.get(url)
            r.raise_for_status()

    timing = monitor.timing
    increment = monitor.increment
    assert increment.call_count == 0
    assert timing.call_count == 1
    assert timing.call_args[0][0] == 'remote_http_call.timer'


def test_trace_remote_http_call_501(req_mocker, mocker):
    req_mocker.get('/test', status_code=501)
    monitor = mocker.patch('huskar_api.extras.utils.monitor_client')
    url = 'http://example.com/test'
    with pytest.raises(requests.HTTPError):
        with trace_remote_http_call(url):
            r = requests.get(url)
            r.raise_for_status()

    timing = monitor.timing
    increment = monitor.increment
    assert increment.call_count == 1
    assert increment.call_args[0][0] == 'remote_http_call.error'
    assert increment.call_args[1]['tags'] == {
        'domain': 'example.com',
        'status_code': '501',
        'type': 'internal_error',
    }

    assert timing.call_count == 1
    assert timing.call_args[0][0] == 'remote_http_call.timer'


@pytest.mark.parametrize('error_type', ['timeout', 'connection_error'])
def test_trace_remote_http_call_conn_error(req_mocker, mocker, error_type):
    if error_type == 'timeout':
        error = requests.Timeout
    else:
        error = requests.ConnectionError
    req_mocker.get('/test', exc=error)
    monitor = mocker.patch('huskar_api.extras.utils.monitor_client')
    url = 'http://example.com/test'
    with pytest.raises(error):
        with trace_remote_http_call(url):
            r = requests.get(url)
            r.raise_for_status()

    timing = monitor.timing
    increment = monitor.increment
    assert increment.call_count == 1
    assert increment.call_args[0][0] == 'remote_http_call.error'
    assert increment.call_args[1]['tags'] == {
        'domain': 'example.com',
        'status_code': 'unknown',
        'type': error_type,
    }

    assert timing.call_count == 1
    assert timing.call_args[0][0] == 'remote_http_call.timer'


def test_huskar_audit_log(mocker, mock_switches):
    mock_switches({SWITCH_ENABLE_AUDIT_LOG: False})

    with huskar_audit_log(action_types.UPDATE_SERVICE_INFO,
                          application_name='test'):
        pass

    mock_switches({SWITCH_ENABLE_AUDIT_LOG: True,
                   SWITCH_ENABLE_MINIMAL_MODE: True})

    with huskar_audit_log(action_types.UPDATE_SERVICE_INFO,
                          application_name='test'):
        pass

    mock_switches({SWITCH_ENABLE_AUDIT_LOG: True,
                   SWITCH_ENABLE_MINIMAL_MODE: False})

    with huskar_audit_log(action_types.UPDATE_CONFIG,
                          application_name='foo.test', cluster_name='overall',
                          key='test', new_data='x' * 65536):
        pass

    session = mocker.patch('huskar_api.models.audit.audit.DBSession')
    session.side_effect = RuntimeError(None, None, None, None)

    with huskar_audit_log(action_types.UPDATE_SERVICE_INFO,
                          application_name='test'):
        pass

    session = mocker.patch('huskar_api.models.audit.audit.DBSession')
    session.side_effect = OperationalError(None, None, None, None)

    with huskar_audit_log(action_types.UPDATE_SERVICE_INFO,
                          application_name='test'):
        pass
