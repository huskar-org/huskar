from __future__ import absolute_import

import logging

from flask import Blueprint, request, abort

from huskar_api import settings
from huskar_api.extras.rate_limiter import (
    check_new_request, RateExceededError)
from huskar_api.switch import switch, SWITCH_ENABLE_RATE_LIMITER


bp = Blueprint('middlewares.rate_limit_ip', __name__)
logger = logging.getLogger(__name__)


@bp.before_app_request
def check_rate_limit():
    if not switch.is_switched_on(SWITCH_ENABLE_RATE_LIMITER):
        return

    remote_addr = request.remote_addr
    config = get_limiter_config(settings.RATE_LIMITER_SETTINGS, remote_addr)
    if not config:
        return

    rate, capacity = config['rate'], config['capacity']
    try:
        check_new_request(remote_addr, rate, capacity)
    except RateExceededError:
        abort(429, 'Too Many Requests, the rate limit is {}/s'.format(rate))


def get_limiter_config(configs, remote_addr):
    if remote_addr in configs:
        return configs[remote_addr]
    if '__anonymous__' in configs:
        return configs['__anonymous__']
    return configs.get('__default__')
