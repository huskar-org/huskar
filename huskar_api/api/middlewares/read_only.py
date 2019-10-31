from __future__ import absolute_import

import logging

from flask import Blueprint, request

from huskar_api import settings
from huskar_api.api.utils import (
    api_response, config_and_switch_readonly_endpoints)
from huskar_api.switch import (
    switch, SWITCH_ENABLE_CONFIG_AND_SWITCH_WRITE)

bp = Blueprint('middlewares.read_only', __name__)
logger = logging.getLogger(__name__)
READ_METHOD_SET = frozenset({'GET', 'HEAD', 'OPTION'})


@bp.before_app_request
def check_config_and_switch_read_only():
    method = request.method
    view_args = request.view_args
    appid = view_args and view_args.get('application_name')

    response = api_response(
        message='Config and switch write inhibit',
        status="Forbidden")
    response.status_code = 403

    if method in READ_METHOD_SET:
        return
    if request.endpoint not in config_and_switch_readonly_endpoints:
        return
    if appid and appid in settings.CONFIG_AND_SWITCH_READONLY_BLACKLIST:
        return response
    if switch.is_switched_on(SWITCH_ENABLE_CONFIG_AND_SWITCH_WRITE, True):
        return
    if appid and appid in settings.CONFIG_AND_SWITCH_READONLY_WHITELIST:
        return
    return response
