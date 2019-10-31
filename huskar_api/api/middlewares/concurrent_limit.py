from __future__ import absolute_import

import logging

from flask import Blueprint, request, g, abort

from huskar_api import settings
from huskar_api.extras.concurrent_limiter import (
    check_new_request, release_request, ConcurrencyExceededError)
from huskar_api.switch import switch, SWITCH_ENABLE_CONCURRENT_LIMITER

bp = Blueprint('middlewares.concurrent_limit', __name__)
logger = logging.getLogger(__name__)


@bp.before_app_request
def check_concurrent_limit():
    if not switch.is_switched_on(SWITCH_ENABLE_CONCURRENT_LIMITER):
        return

    if g.get('auth'):
        anonymous = False
        username = g.auth.username
    else:
        anonymous = True
        username = request.remote_addr
    config = get_limiter_config(
        settings.CONCURRENT_LIMITER_SETTINGS, username, anonymous=anonymous)
    if not config:
        return

    ttl, capacity = config['ttl'], config['capacity']
    try:
        result = check_new_request(username, ttl, capacity)
    except ConcurrencyExceededError:
        abort(429, 'Too Many Requests, only allow handling {} requests '
              'in {} seconds'.format(capacity, ttl))
    else:
        if result is not None:
            key, sub_item = result
            g.concurrent_limiter_data = {'key': key, 'sub_item': sub_item}


@bp.after_app_request
def release_concurrent_limiter_data(response):
    if (g.get('concurrent_limiter_data') and
            (response.status_code != 200 or
             request.endpoint != 'api.long_polling')):
        data = g.concurrent_limiter_data
        release_request(data['key'], data['sub_item'])
        g.concurrent_limiter_data = None

    return response


def get_limiter_config(configs, username, anonymous):
    if username in configs:
        return configs[username]
    if anonymous and '__anonymous__' in configs:
        return configs['__anonymous__']
    return configs.get('__default__')
