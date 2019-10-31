from __future__ import absolute_import

from huskar_sdk_v2.consts import (
    SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN)

from huskar_api.service.service import ServiceLink
from huskar_api.models.exceptions import DataConflictError
from huskar_api.models.route import RouteManagement
from huskar_api.models import huskar_client
from huskar_api.models.instance import InfraInfo
from huskar_api.service import service as service_facade
from huskar_api.service import switch as switch_facade
from huskar_api.service import config as config_facade
from .action import action_types

_FACADE_TYPE_MAP = {
    SERVICE_SUBDOMAIN: service_facade,
    CONFIG_SUBDOMAIN: config_facade,
    SWITCH_SUBDOMAIN: switch_facade
}


class ActionRollback(object):

    def __init__(self):
        self._funcs = {}

    def __call__(self, action_type):
        def decorator(func):
            assert action_type not in self._funcs
            self._funcs[action_type] = func
            return func
        return decorator

    def rollback(self, action_type, action_data):
        handler = self._funcs[action_type]
        return handler(action_type, action_data)

    def can_rollback(self, action_type):
        return action_type in self._funcs


action_rollback = ActionRollback()


@action_rollback(action_types.UPDATE_SERVICE)
@action_rollback(action_types.UPDATE_SWITCH)
@action_rollback(action_types.UPDATE_CONFIG)
@action_rollback(action_types.DELETE_SWITCH)
@action_rollback(action_types.DELETE_SERVICE)
@action_rollback(action_types.DELETE_CONFIG)
def rollback_instance_configuration(action_type, action_data):
    _, data_type = action_types[action_type].split('_', 1)
    facade = _FACADE_TYPE_MAP[data_type.lower()]
    application_name = action_data['application_name']
    cluster_name = action_data['cluster_name']
    key = action_data['key']
    now_data = facade.get_value(application_name, cluster_name, key)
    old_data = action_data['data']['old']

    if old_data is not None:
        facade.create(
            application=application_name,
            cluster=cluster_name,
            key=key,
            value=old_data
        )
        new_action_type = getattr(
            action_types, '_'.join(['UPDATE', data_type]))
    else:
        facade.delete(application_name, cluster_name, key, strict=True)
        new_action_type = getattr(
            action_types, '_'.join(['DELETE', data_type]))
    return new_action_type, {
        'application_name': application_name,
        'cluster_name': cluster_name,
        'key': key,
        'old_data': now_data,
        'new_data': old_data
    }


@action_rollback(action_types.ASSIGN_CLUSTER_LINK)
@action_rollback(action_types.DELETE_CLUSTER_LINK)
def rollback_cluster_link_change(action_type, action_data):
    application_name = action_data['application_name']
    cluster_name = action_data['cluster_name']
    if action_type == action_types.DELETE_CLUSTER_LINK:
        if ServiceLink.get_link(application_name, cluster_name):
            raise DataConflictError()
        link = action_data['physical_name']
        ServiceLink.set_link(application_name, cluster_name, link)
        new_action_type = action_types.ASSIGN_CLUSTER_LINK
    else:
        link = ServiceLink.get_link(application_name, cluster_name)
        if link != action_data['physical_name']:
            raise DataConflictError()
        ServiceLink.delete_link(application_name, cluster_name)
        new_action_type = action_types.DELETE_CLUSTER_LINK
    return new_action_type, {
        'application_name': application_name,
        'cluster_name': cluster_name,
        'physical_name': link
    }


@action_rollback(action_types.UPDATE_ROUTE)
@action_rollback(action_types.DELETE_ROUTE)
def rollback_route_action(action_type, action_data):
    application_name = action_data['application_name']
    cluster_name = action_data['cluster_name']
    intent = action_data['intent']
    dest_application_name = action_data['dest_application_name']
    dest_cluster_name = action_data.get('dest_cluster_name')
    rm = RouteManagement(huskar_client, application_name, cluster_name)
    if action_type == action_types.DELETE_ROUTE:
        rm.set_route(dest_application_name, dest_cluster_name)
        new_action_type = action_types.UPDATE_ROUTE
    else:
        rm.discard_route(dest_application_name)
        new_action_type = action_types.DELETE_ROUTE
    return new_action_type, {
        'application_name': application_name,
        'cluster_name': cluster_name,
        'intent': intent,
        'dest_application_name': dest_application_name,
        'dest_cluster_name': dest_cluster_name
    }


@action_rollback(action_types.UPDATE_INFRA_CONFIG)
@action_rollback(action_types.DELETE_INFRA_CONFIG)
def rollback_infra_config(action_type, action_data):
    application_name = action_data['application_name']
    infra_type = action_data['infra_type']
    infra_name = action_data['infra_name']
    scope_type = action_data['scope_type']
    scope_name = action_data['scope_name']

    infra_info = InfraInfo(
        huskar_client.client, application_name, infra_type)
    infra_info.load()
    current = infra_info.get_by_name(infra_name, scope_type, scope_name)
    rollback_to = action_data['data']['old']

    if rollback_to is None:  # new infra config
        infra_info.delete_by_name(infra_name, scope_type, scope_name)
        new_action_type = action_types.DELETE_INFRA_CONFIG
    else:
        infra_info.set_by_name(
            infra_name, scope_type, scope_name, rollback_to)
        new_action_type = action_types.UPDATE_INFRA_CONFIG
    infra_info.save()

    return new_action_type, {
        'application_name': application_name,
        'infra_type': infra_type,
        'infra_name': infra_name,
        'scope_type': scope_type,
        'scope_name': scope_name,
        'old_value': current,
        'new_value': rollback_to,
    }
