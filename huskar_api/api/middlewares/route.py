from __future__ import absolute_import

from flask import Blueprint, request, g, abort

from huskar_api.models.const import ROUTE_MODES
from huskar_api.extras.monitor import monitor_client


bp = Blueprint('middlewares.route', __name__)


@bp.before_app_request
def collect_route_mode():
    frontend_name = request.headers.get('X-Frontend-Name')
    mode = request.headers.get('X-SOA-Mode')
    if mode and mode not in ROUTE_MODES:
        abort(400, u'X-SOA-Mode must be one of %s' % u'/'.join(ROUTE_MODES))
    if not mode:
        mode = 'unknown'
    g.route_mode = mode
    if not frontend_name and g.auth.username and g.auth.is_application:
        monitor_client.increment('route_mode.qps', tags=dict(
            mode=mode, from_user=g.auth.username, appid=g.auth.username))


@bp.before_app_request
def collect_application_name():
    g.cluster_name = request.headers.get('X-Cluster-Name')
    if g.auth.username and g.auth.is_application:
        monitor_client.increment('route_mode.cluster', tags=dict(
            from_cluster=g.cluster_name or 'unknown',
            from_user=g.auth.username, appid=g.auth.username))
    if g.auth and g.auth.is_application and g.route_mode == 'route':
        g.application_name = g.auth.username
        if not g.cluster_name:
            abort(400, u'X-Cluster-Name is required while X-SOA-Mode is route')
    else:
        g.application_name = None
