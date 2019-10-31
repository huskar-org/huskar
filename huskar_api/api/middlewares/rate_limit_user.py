from __future__ import absolute_import

import logging

from flask import Blueprint, g, abort

from huskar_api import settings
from huskar_api.extras.rate_limiter import (
    check_new_request, RateExceededError)
from huskar_api.switch import switch, SWITCH_ENABLE_RATE_LIMITER


bp = Blueprint('middlewares.rate_limit_user', __name__)
logger = logging.getLogger(__name__)


@bp.before_app_request
def check_rate_limit():
    if not switch.is_switched_on(SWITCH_ENABLE_RATE_LIMITER):
        return
    if not g.get('auth'):
        return

    username = g.auth.username
    config = get_limiter_config(settings.RATE_LIMITER_SETTINGS, username)
    if not config:
        return

    rate, capacity = config['rate'], config['capacity']
    try:
        check_new_request(username, rate, capacity)
    except RateExceededError:
        abort(429, 'Too Many Requests, the rate limit is {}/s'.format(rate))


def get_limiter_config(configs, username):
    if username in configs:
        return configs[username]
    return configs.get('__default__')
