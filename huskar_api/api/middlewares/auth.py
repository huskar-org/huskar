from __future__ import absolute_import

import logging

from flask import Blueprint, request, g, abort

from huskar_api import settings
from huskar_api.ext import db_tester
from huskar_api.extras.monitor import monitor_client
from huskar_api.models.auth import SessionAuth
from huskar_api.models.const import MM_REASON_TESTER, MM_REASON_STARTUP
from huskar_api.extras.uptime import process_uptime
from huskar_api.service.admin.application_auth import (
    is_application_blacklisted)


bp = Blueprint('middlewares.auth', __name__)
logger = logging.getLogger(__name__)


@bp.before_app_request
def check_blacklist():
    if request.remote_addr not in settings.AUTH_IP_BLACKLIST:
        return
    abort(403, 'The IP address is blacklisted')


@bp.before_app_request
def authenticate():
    token = request.headers.get('Authorization', '').strip()
    g.auth = SessionAuth.from_token(token)
    g.auth.load_user()
    if not db_tester.ok:
        g.auth.enter_minimal_mode(MM_REASON_TESTER)
    if (settings.MM_GRACEFUL_STARTUP_TIME and
            process_uptime() <= settings.MM_GRACEFUL_STARTUP_TIME):
        g.auth.enter_minimal_mode(MM_REASON_STARTUP)


@bp.before_app_request
def check_application_blacklist():
    if is_application_blacklisted(g.auth.username):
        abort(403, 'application: {} is blacklisted'.format(g.auth.username))


@bp.before_app_request
def detect_token_abuse():
    frontend_name = request.headers.get('X-Frontend-Name')
    if (g.auth.is_application and
            frontend_name and frontend_name == settings.ADMIN_FRONTEND_NAME):
        abort(403, 'Using application token in web is not permitted.')


@bp.after_app_request
def track_user_qps(response):
    if not request.endpoint:
        return response

    if g.get('auth'):
        name = g.auth.username
        kind = 'app' if g.auth.is_application else 'user'
    else:
        name = 'anonymous'
        kind = 'anonymous'
    tags = dict(kind=kind, name=name)
    if kind == 'app':
        tags.update(appid=name)
    monitor_client.increment('qps.all', tags=tags)
    monitor_client.increment('qps.url', tags=dict(
        endpoint=request.endpoint, method=request.method, **tags))

    return response


@bp.after_app_request
def indicate_minimal_mode(response):
    auth = g.get('auth')
    if auth is not None and auth.is_minimal_mode:
        response.headers['X-Minimal-Mode'] = u'1'
        response.headers['X-Minimal-Mode-Reason'] = \
            unicode(auth.minimal_mode_reason or u'')
    return response
