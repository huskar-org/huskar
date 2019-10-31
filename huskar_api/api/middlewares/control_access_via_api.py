from __future__ import absolute_import

import logging

from flask import Blueprint, request, g

from huskar_api import settings
from huskar_api.api.utils import api_response
from huskar_api.extras.monitor import monitor_client
from huskar_api.switch import (
    switch, SWITCH_DISABLE_FETCH_VIA_API, SWITCH_DISABLE_UPDATE_VIA_API)

logger = logging.getLogger(__name__)
bp = Blueprint('middlewares.control_access_via_api', __name__)
FETCH_METHOD_SET = frozenset({'GET', 'HEAD', 'OPTION'})


@bp.before_app_request
def check_access_via_api():
    frontend_name = request.headers.get('X-Frontend-Name')
    if frontend_name == settings.ADMIN_FRONTEND_NAME:
        return
    if request.endpoint is None:
        return

    method = request.method
    endpoint = request.endpoint
    username = g.auth.username
    message = (
        'Request this api is forbidden, please access huskar console instead')
    response = api_response(message=message, status='Forbidden')
    response.status_code = 403

    action = 'fetch'
    if method in FETCH_METHOD_SET:
        action = 'fetch'
        trace_access(g.auth, endpoint, action, 'all')
        if allow_fetch_api(username, endpoint):
            return
    else:
        action = 'update'
        trace_access(g.auth, endpoint, action, 'all')
        if allow_update_api(username, endpoint):
            return

    trace_access(g.auth, endpoint, action, 'forbidden')
    return response


def allow_fetch_api(username, endpoint):
    action = 'fetch'
    if not switch.is_switched_on(SWITCH_DISABLE_FETCH_VIA_API, False):
        return True

    if is_allow(
            username, endpoint, settings.ALLOW_FETCH_VIA_API_USERS, action):
        return True

    return False


def allow_update_api(username, endpoint):
    action = 'update'
    if not switch.is_switched_on(SWITCH_DISABLE_UPDATE_VIA_API, False):
        return True

    if is_allow(
            username, endpoint, settings.ALLOW_UPDATE_VIA_API_USERS, action):
        return True

    return False


def is_allow(username, endpoint, allow_config, action):
    if endpoint in settings.ALLOW_ALL_VIA_API_ENDPOINTS.get(action, []):
        return True

    allow_users = allow_config.get(endpoint, [])
    if '*' in allow_users:
        return True
    if username in allow_users:
        return True

    return False


def trace_access(auth, endpoint, action, key):
    if not auth.is_application:
        return
    if endpoint in settings.ALLOW_ALL_VIA_API_ENDPOINTS.get(action, []):
        return

    monitor_client.increment('access_via_api.{}'.format(key), tags={
        'appid': auth.username,
        'endpoint': endpoint,
        'action': action,
    })
