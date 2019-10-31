from __future__ import absolute_import

import json
import time
import logging

from flask import request, g, Blueprint

from huskar_api.extras.monitor import monitor_client


bp = Blueprint('middlewares.logger', __name__)
logger = logging.getLogger(__name__)


COMMON_SENSITIVE_FIELDS = frozenset([
    'password',
    'old_password',
    'new_password',
    'value',
])
INFRA_CONFIG_SENSITIVE_FIELDS = frozenset([
    'master',
    'slave',
    'url',
])


def get_request_user():
    if g.auth:
        return '%s %s' % (g.auth.username, request.remote_addr)
    else:
        return 'anonymous_user %s' % request.remote_addr


def get_request_args():
    args = {}
    args.update(request.view_args.items())
    args.update(request.values.items())
    json_body = request.get_json(silent=True) or {}
    if isinstance(json_body, dict):
        args.update(json_body)

    sensitive_fields = set(COMMON_SENSITIVE_FIELDS)
    if request.endpoint == 'api.infra_config':
        sensitive_fields.update(INFRA_CONFIG_SENSITIVE_FIELDS)

    return {k: v for k, v in args.items() if k not in sensitive_fields}


@bp.before_app_request
def start_profiling():
    g._start_timestamp = int(time.time() * 1000)


@bp.after_app_request
def record_access_log(response):
    if not g.get('_start_timestamp') or request.endpoint in (
            None, 'api.health_check'):
        return response

    time_usage = int(time.time() * 1000) - g._start_timestamp
    is_ok = (response.status_code // 100) in (2, 3)
    api_status = getattr(response, '_api_status', 'unknown')

    # use JSON string to follow standard Tokenizer of ELK
    # elastic.co/guide/cn/elasticsearch/guide/current/standard-tokenizer.html
    logger.info(
        'Call %s: %s call <%s %s> %s, time: %sms, soa_mode: %s, cluster: %s, '
        'status: %s, status_code: %s',
        'Ok' if is_ok else 'Failed',
        get_request_user(), request.method, request.path,
        json.dumps(get_request_args()), time_usage, g.get('route_mode'),
        g.get('cluster_name') or 'unknown', api_status, response.status_code)

    tags = dict(method=request.method,
                endpoint=request.endpoint,
                status=api_status,
                status_code=str(response.status_code))
    monitor_client.timing('api_response', time_usage, tags=tags)

    return response
