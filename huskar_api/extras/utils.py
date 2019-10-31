from __future__ import absolute_import

import contextlib
import logging
import time
import urlparse

from requests import Timeout, ConnectionError, HTTPError

from huskar_api import settings
from huskar_api.switch import (
    switch, SWITCH_ENABLE_AUDIT_LOG, SWITCH_ENABLE_MINIMAL_MODE)
from huskar_api.extras.monitor import monitor_client
from huskar_api.extras.raven import capture_message, capture_exception
from huskar_api.models.auth import User
from huskar_api.models.audit import (
    AuditLog, action_types, action_creator, logger as fallback_audit_logger)
from huskar_api.models.exceptions import (
    AuditLogTooLongError, AuditLogLostError)

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def trace_remote_http_call(url):
    start_at = int(time.time() * 1000)
    domain = urlparse.urlparse(url).hostname
    try:
        yield
    except HTTPError as e:
        response = e.response
        status_code = response.status_code
        if response.status_code >= 500:
            monitor_client.increment(
                'remote_http_call.error', tags={
                    'type': 'internal_error',
                    'domain': domain,
                    'status_code': str(status_code),
                })
            message = 'Remote HTTP API Internal Server Error'
            capture_message(message, level=logging.WARNING, extra={
                'url': url,
                'status_code': status_code,
                'body': repr(response.content),
            })
        raise
    except (Timeout, ConnectionError) as e:
        if isinstance(e, Timeout):
            _type = 'timeout'
        else:
            _type = 'connection_error'
        monitor_client.increment(
            'remote_http_call.error', tags={
                'type': _type,
                'domain': domain,
                'status_code': 'unknown',
            })
        capture_exception(level=logging.WARNING, extra={'url': url})
        raise
    finally:
        monitor_client.timing(
            'remote_http_call.timer', int(time.time() * 1000) - start_at,
            tags={'domain': domain})


@contextlib.contextmanager
def huskar_audit_log(action_type, **extra):
    if not switch.is_switched_on(SWITCH_ENABLE_AUDIT_LOG):
        yield
        return
    action = action_creator.make_action(action_type, **extra)
    yield
    try:
        if switch.is_switched_on(SWITCH_ENABLE_MINIMAL_MODE, False):
            action_name = action_types[action_type]
            fallback_audit_logger.info(
                'arch.huskar_api %s %r', action_name, action.action_data)
        else:
            user = User.get_by_name('arch.huskar_api')
            user_id = user.id if user else 0
            AuditLog.create(user_id, settings.LOCAL_REMOTE_ADDR, action)
    except AuditLogTooLongError:
        logger.info('Audit log is too long. %s arch.huskar_api',
                    action_types[action_type])
        return
    except AuditLogLostError:
        action_name = action_types[action_type]
        fallback_audit_logger.info(
            'arch.huskar %s %r', action_name, action.action_data)
    except Exception:
        logger.exception('Unexpected error of audit log')
