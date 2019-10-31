from __future__ import absolute_import

from flask import Blueprint
from huskar_sdk_v2.consts import (
    SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN)

from huskar_api.service import service as service_facade
from huskar_api.service import switch as switch_facade
from huskar_api.service import config as config_facade
from huskar_api.models.audit import AuditLog
from .utils import config_and_switch_readonly_endpoints
from .auth import (
    HuskarAdminView, HuskarTokenView, ApplicationAuthView,
    TeamAdminView)
from .health_check import HealthCheckView
from .long_polling import LongPollingView
from .organization import (
    TeamView, ApplicationView, ApplicationListView, ApplicationTokenView,
    TeamApplicationTokenView)
from .instance import InstanceView, InstanceBatchView, ClusterView
from .infra_config import InfraConfigView, InfraConfigDownstreamView
from .service_instance import (
    ServiceInstanceView, ServiceInstanceWeightView, ServiceRegistryView)
from .service_info import ServiceInfoView, ClusterInfoView
from .service_link import ServiceLinkView
from .service_route import ServiceRouteView, ServiceDefaultRouteView
from .user import UserView, PasswordResetView
from .audit import AuditLogView, AuditRollbackView, AuditTimelineView
from .webhook import WebhookView, WebhookInstanceView, ApplicationWebhookView
from .support import (
    CurrentUserView, ContainerRegistryView, BlacklistView, RouteProgramView)
from .well_known import WellKnownCommonView

bp = Blueprint('api', __name__)


def add_route(url, view_func, **kwargs):
    return bp.add_url_rule(url, view_func=view_func, **kwargs)


add_route('/whoami', CurrentUserView.as_view('whoami'))
add_route('/.well-known/common', WellKnownCommonView.as_view(
    'well_known_common'))


add_route('/service/<application_name>/<cluster_name>',
          ServiceInstanceView.as_view('service'))
add_route('/switch/<application_name>/<cluster_name>',
          InstanceView.as_view('switch', SWITCH_SUBDOMAIN, switch_facade,
                               is_public=True))
config_and_switch_readonly_endpoints.add('api.switch')
add_route('/config/<application_name>/<cluster_name>',
          InstanceView.as_view('config', CONFIG_SUBDOMAIN, config_facade,
                               is_public=False))
config_and_switch_readonly_endpoints.add('api.config')
add_route('/service/<application_name>/<cluster_name>/<key>/weight',
          ServiceInstanceWeightView.as_view('service_weight'))


service_route_view = ServiceRouteView.as_view('service_route')
service_link_view = ServiceLinkView.as_view('service_link')
default_route_view = ServiceDefaultRouteView.as_view('service_default_route')
add_route('/serviceroute/default/<application_name>', default_route_view)
add_route('/serviceroute/<application_name>/<cluster_name>',
          service_route_view, methods=['GET'])
add_route('/serviceroute/<application_name>/<cluster_name>/<destination>',
          service_route_view, methods=['PUT', 'DELETE'])
add_route('/servicelink/<application_name>/<cluster_name>',
          service_link_view)


add_route('/serviceinfo/<application_name>',
          ServiceInfoView.as_view('service_info'))
add_route('/serviceinfo/<application_name>/<cluster_name>',
          ClusterInfoView.as_view('cluster_info'))

add_route('/service/<application_name>',
          ClusterView.as_view('service_cluster', SERVICE_SUBDOMAIN,
                              service_facade, is_public=True))
add_route('/switch/<application_name>',
          ClusterView.as_view('switch_cluster', SWITCH_SUBDOMAIN,
                              switch_facade, is_public=True))
config_and_switch_readonly_endpoints.add('api.switch_cluster')
add_route('/config/<application_name>',
          ClusterView.as_view('config_cluster', CONFIG_SUBDOMAIN,
                              config_facade, is_public=False))
config_and_switch_readonly_endpoints.add('api.config_cluster')


add_route('/batch_service',
          InstanceBatchView.as_view(
              'service_batch', SERVICE_SUBDOMAIN, service_facade,
              is_public=True, has_comment=False))
add_route('/batch_switch',
          InstanceBatchView.as_view(
              'switch_batch', SWITCH_SUBDOMAIN, switch_facade,
              is_public=True, has_comment=True))
config_and_switch_readonly_endpoints.add('api.batch_switch')
add_route('/batch_config',
          InstanceBatchView.as_view(
              'config_batch', CONFIG_SUBDOMAIN, config_facade,
              is_public=False, has_comment=True))
config_and_switch_readonly_endpoints.add('api.batch_config')


user_view = UserView.as_view('user')
add_route('/user', user_view, methods=['GET', 'POST'])
add_route('/user/<username>', user_view, methods=['GET', 'PUT', 'DELETE'])
add_route('/user/<username>/password-reset',
          PasswordResetView.as_view('password_reset'))


admin_view = HuskarAdminView.as_view('huskar_admin')
add_route('/auth/huskar', admin_view, methods=['POST'])
add_route('/auth/huskar/<username>', admin_view, methods=['DELETE'])
add_route('/auth/token', HuskarTokenView.as_view('huskar_token'))
add_route('/auth/application/<application_name>',
          ApplicationAuthView.as_view('application_auth'))
add_route('/auth/team/<team_name>', TeamAdminView.as_view('team_admin'))


team_view = TeamView.as_view('team')
add_route('/team', team_view, methods=['GET', 'POST'])
add_route('/team/<team_name>', team_view, methods=['GET', 'DELETE'])
add_route('/team/<team_name>/application/<application_name>/token',
          TeamApplicationTokenView.as_view('team_application_token'),
          methods=['POST'])
add_route('/application', ApplicationListView.as_view('application'),
          methods=['GET', 'POST', 'PUT'])
add_route('/application/<application_name>',
          ApplicationView.as_view('application_item'),
          methods=['GET', 'DELETE'])
add_route('/application/<application_name>/token',
          ApplicationTokenView.as_view('application_token'))


service_registry_view = ServiceRegistryView.as_view('service_registry')
long_polling_view = LongPollingView.as_view('long_polling')
add_route('/data/service-registry', service_registry_view)
add_route('/data/long-polling', long_polling_view)
add_route('/data/long_poll', long_polling_view)  # TODO deprecated

add_route('/health_check', HealthCheckView.as_view('health_check'))


add_route('/audit/site',
          AuditLogView.as_view('audit_site', AuditLog.TYPE_SITE))
add_route('/audit/team/<name>',
          AuditLogView.as_view('audit_team', AuditLog.TYPE_TEAM))
add_route('/audit/application/<name>',
          AuditLogView.as_view('audit_application', AuditLog.TYPE_APPLICATION))
add_route('/audit-rollback/<application_name>/<audit_id>',
          AuditRollbackView.as_view('audit_rollback'))
config_and_switch_readonly_endpoints.add('api.audit_rollback')
add_route('/audit-timeline/config/<application_name>/<cluster_name>/<key>',
          AuditTimelineView.as_view(
              'config_timeline', AuditLog.TYPE_CONFIG))
add_route('/audit-timeline/switch/<application_name>/<cluster_name>/<key>',
          AuditTimelineView.as_view(
              'switch_timeline', AuditLog.TYPE_SWITCH))
add_route('/audit-timeline/service/<application_name>/<cluster_name>/<key>',
          AuditTimelineView.as_view(
              'service_timeline', AuditLog.TYPE_SERVICE))


add_route('/webhook', WebhookView.as_view('webhook'), methods=['GET', 'POST'])
add_route('/webhook/<int:webhook_id>', WebhookInstanceView.as_view(
    'webhook_instance'), methods=['GET', 'DELETE', 'PUT'])
add_route('/webhook/application/<application_name>',
          ApplicationWebhookView.as_view('application_webhook'),
          methods=['GET'])

add_route('/_internal/arch/route-program',
          RouteProgramView.as_view('internal_route_program'))
config_and_switch_readonly_endpoints.add('api.internal_route_program')
add_route('/_internal/tools/container/registry/<container_id>',
          ContainerRegistryView.as_view('internal_container_registry'))
add_route('/_internal/ops/blacklist',
          BlacklistView.as_view('internal_blacklist'))

add_route('/infra-config/<application_name>/<infra_type>/<infra_name>',
          InfraConfigView.as_view('infra_config'))
config_and_switch_readonly_endpoints.add('api.infra_config')
add_route('/infra-config-downstream/<infra_application_name>',
          InfraConfigDownstreamView.as_view('infra_config_downstream'))
