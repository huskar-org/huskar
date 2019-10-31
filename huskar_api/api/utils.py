from __future__ import absolute_import

import datetime
import random
import logging
import functools
import contextlib

from flask import abort, g, request, jsonify

from huskar_api import settings
from huskar_api.ext import sentry
from huskar_api.models.audit import (
    AuditLog, action_types, action_creator, logger as fallback_audit_logger)
from huskar_api.models.exceptions import (
    AuditLogTooLongError, AuditLogLostError)
from huskar_api.switch import (
    switch,
    SWITCH_ENABLE_AUDIT_LOG,
    SWITCH_ENABLE_LONG_POLLING_MAX_LIFE_SPAN)
from huskar_api.extras.email import EmailDeliveryError, deliver_email

logger = logging.getLogger(__name__)

config_and_switch_readonly_endpoints = set()


def api_response(data=None, status=u'SUCCESS', message=u''):
    response = jsonify(data=data, status=status, message=message)
    mark_api_status_on_response(response, status=status)
    return response


def mark_api_status_on_response(response, status=u'SUCCESS'):
    response._api_status = status
    return response


def login_required(wrapped):
    @functools.wraps(wrapped)
    def wrapper(*args, **kwargs):
        if g.auth:
            return wrapped(*args, **kwargs)
        abort(401, 'The token is missing, invalid or expired.')
    wrapper.original = getattr(wrapped, 'original', wrapped)
    return wrapper


def with_etag(wrapped):
    @functools.wraps(wrapped)
    def wrapper(*args, **kwargs):
        response = wrapped(*args, **kwargs)
        response.add_etag()
        return response.make_conditional(request)
    return wrapper


def with_cache_control(wrapped):
    @functools.wraps(wrapped)
    def wrapper(*args, **kwargs):
        response = wrapped(*args, **kwargs)
        if g.auth.is_minimal_mode:
            return response
        directives = settings.CACHE_CONTROL_SETTINGS.get(request.endpoint, {})
        for name, value in directives.items():
            setattr(response.cache_control, name, value)
        return response
    return wrapper


def minimal_mode_incompatible(wrapped):
    @functools.wraps(wrapped)
    def wrapper(*args, **kwargs):
        if not g.auth.is_minimal_mode:
            return wrapped(*args, **kwargs)
        abort(501, 'Current API is not suitable for working in minimal mode')
    wrapper.original = getattr(wrapped, 'original', wrapped)
    return wrapper


@contextlib.contextmanager
def audit_log(action_type, **extra):
    if not switch.is_switched_on(SWITCH_ENABLE_AUDIT_LOG):
        yield
        return
    action = action_creator.make_action(action_type, **extra)
    yield
    try:
        if g.auth.is_minimal_mode:
            action_name = action_types[action_type]
            fallback_audit_logger.info(
                '%s %s %r', g.auth.username, action_name, action.action_data)
        else:
            user_id = g.auth.id if g.auth else 0
            AuditLog.create(user_id, request.remote_addr, action)
    except AuditLogTooLongError:
        logger.info('Audit log is too long. %s %s %s',
                    action_types[action_type], g.auth.username,
                    request.remote_addr)
        return
    except AuditLogLostError:
        action_name = action_types[action_type]
        fallback_audit_logger.info(
            '%s %s %r', g.auth.username, action_name, action.action_data)
        sentry.captureException(level=logging.WARNING)
    except Exception:
        logger.exception('Unexpected error of audit log')
        sentry.captureException()


def emit_audit_log(action_type, **extra):
    with audit_log(action_type, **extra):
        pass


audit_log.types = action_types
audit_log.emit = emit_audit_log


def deliver_email_safe(*args, **kwargs):
    try:
        deliver_email(*args, **kwargs)
    except EmailDeliveryError:
        logger.exception('Failed to deliver Email')


def strptime2date(date_str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()


def get_life_span(old_life_span):
    if not switch.is_switched_on(SWITCH_ENABLE_LONG_POLLING_MAX_LIFE_SPAN):
        return old_life_span
    if g.auth.username in settings.LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE:
        return old_life_span

    max_life_span = settings.LONG_POLLING_MAX_LIFE_SPAN
    life_span_jitter = settings.LONG_POLLING_LIFE_SPAN_JITTER
    if 0 < old_life_span < life_span_jitter:
        return old_life_span
    new_life_span = min(old_life_span or max_life_span,
                        max_life_span) + random.random() * life_span_jitter
    return new_life_span
