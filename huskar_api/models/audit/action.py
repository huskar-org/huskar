from __future__ import absolute_import

import logging
import contextlib
import itertools
from collections import namedtuple

from huskar_api.models.auth import Application
from huskar_api.models.const import SELF_APPLICATION_NAME
from .const import (TYPE_SITE, TYPE_TEAM, TYPE_APPLICATION,
                    TYPE_CONFIG, TYPE_SWITCH, TYPE_SERVICE)


__all__ = ['action_types', 'action_creator']

logger = logging.getLogger(__name__)

INSTANCE_TYPE_MAP = {
    'config': TYPE_CONFIG,
    'switch': TYPE_SWITCH,
    'service': TYPE_SERVICE
}


Action = namedtuple('Action', [
    'action_type',
    'action_data',
    'action_indices'
])


class ActionCreator(object):
    """The factory method registry of actions."""

    def __init__(self):
        self._funcs = {}

    def __call__(self, action_type):
        """Registers an action factory."""
        def decorator(func):
            assert action_type not in self._funcs
            self._funcs[action_type] = func
            return func
        return decorator

    def make_action(self, action_type, **extra):
        """Creates an action tuple."""
        func = self._funcs[action_type]
        return Action(action_type, *func(action_type, **extra))


class ActionType(object):
    """The immutable map for audit action types."""

    def __init__(self, action_map):
        self._action_map = {
            name: ident for name, ident in action_map.items()
            if not name.startswith('_')}
        self._action_reversed_map = {
            ident: name for name, ident in self._action_map.items()}

    def __getitem__(self, ident):
        return self._action_reversed_map[ident]

    def __getattribute__(self, name):
        if not name.isupper() or name not in self._action_map:
            return object.__getattribute__(self, name)
        return self._action_map[name]

    def __setattr__(self, name, value):
        if name.isupper():
            raise AttributeError('can not set attribute')
        object.__setattr__(self, name, value)

    @property
    def action_map(self):
        return self._action_map


# XXX NEVER REMOVE ANY ACTION TYPE HERE
# If you want to discard an action type, prefix it an underline instead.
# Don't forget update settings.DANGEROUS_ACTION_TYPES_EXCLUDE_LIST if necessary
action_types = ActionType({
    '_DISCARD_TYPE': -1,

    'CREATE_TEAM': 1001,
    'DELETE_TEAM': 1002,
    'ARCHIVE_TEAM': 1003,
    '_UNARCHIVE_TEAM': 1004,               # reserved
    'CREATE_APPLICATION': 1101,
    'DELETE_APPLICATION': 1102,
    'ARCHIVE_APPLICATION': 1103,
    '_UNARCHIVE_APPLICATION': 1104,        # reserved
    'CREATE_USER': 1201,
    'DELETE_USER': 1202,
    'ARCHIVE_USER': 1203,
    '_UNARCHIVE_USER': 1204,                # reserved
    'CHANGE_USER_PASSWORD': 1205,
    'FORGOT_USER_PASSWORD': 1206,

    'GRANT_HUSKAR_ADMIN': 2001,
    'DISMISS_HUSKAR_ADMIN': 2002,
    'GRANT_TEAM_ADMIN': 2101,
    'DISMISS_TEAM_ADMIN': 2102,
    'GRANT_APPLICATION_AUTH': 2201,
    'DISMISS_APPLICATION_AUTH': 2202,

    '_CREATE_SERVICE': 3001,               # reserved
    'UPDATE_SERVICE': 3002,
    'DELETE_SERVICE': 3003,
    'CREATE_SERVICE_CLUSTER': 3004,
    'DELETE_SERVICE_CLUSTER': 3005,
    'IMPORT_SERVICE': 3006,

    '_CREATE_SWITCH': 3101,                # reserved
    'UPDATE_SWITCH': 3102,
    'DELETE_SWITCH': 3103,
    'CREATE_SWITCH_CLUSTER': 3104,
    'DELETE_SWITCH_CLUSTER': 3105,
    'IMPORT_SWITCH': 3106,
    '_CREATE_CONFIG': 3201,                # reserved
    'UPDATE_CONFIG': 3202,
    'DELETE_CONFIG': 3203,
    'CREATE_CONFIG_CLUSTER': 3204,
    'DELETE_CONFIG_CLUSTER': 3205,
    'IMPORT_CONFIG': 3206,
    'UPDATE_INFRA_CONFIG': 3207,
    'DELETE_INFRA_CONFIG': 3208,
    'UPDATE_SERVICE_INFO': 3301,
    'UPDATE_CLUSTER_INFO': 3302,
    'ASSIGN_CLUSTER_LINK': 3303,
    'DELETE_CLUSTER_LINK': 3304,

    'OBTAIN_USER_TOKEN': 4001,
    'OBTAIN_APPLICATION_TOKEN': 4002,

    'UPDATE_ROUTE': 5001,
    'DELETE_ROUTE': 5002,
    'UPDATE_DEFAULT_ROUTE': 5003,
    'DELETE_DEFAULT_ROUTE': 5004,

    'PROGRAM_UPDATE_ROUTE_STAGE': 8001,
})


action_creator = ActionCreator()


@action_creator(action_types.CREATE_TEAM)
@action_creator(action_types.DELETE_TEAM)
@action_creator(action_types.ARCHIVE_TEAM)
def make_team_action(action_type, team):
    data = {
        'team_id': team.id,
        'team_name': team.team_name,
        'team_desc': team.team_desc,
    }
    return data, [(TYPE_SITE, 0)]


@action_creator(action_types.CREATE_APPLICATION)
@action_creator(action_types.DELETE_APPLICATION)
@action_creator(action_types.ARCHIVE_APPLICATION)
def make_application_action(action_type, application, team):
    data = {
        'application_id': application.id,
        'application_name': application.application_name,
        'team_id': team.id,
        'team_name': team.team_name,
        'team_desc': team.team_desc,
    }
    return data, [(TYPE_SITE, 0), (TYPE_TEAM, team.id)]


@action_creator(action_types.CREATE_USER)
@action_creator(action_types.DELETE_USER)
@action_creator(action_types.ARCHIVE_USER)
@action_creator(action_types.CHANGE_USER_PASSWORD)
@action_creator(action_types.FORGOT_USER_PASSWORD)
@action_creator(action_types.GRANT_HUSKAR_ADMIN)
@action_creator(action_types.DISMISS_HUSKAR_ADMIN)
@action_creator(action_types.OBTAIN_USER_TOKEN)
def make_user_action(action_type, user):
    data = {'username': user.username, 'user_id': user.id}
    return data, [(TYPE_SITE, 0)]


@action_creator(action_types.GRANT_TEAM_ADMIN)
@action_creator(action_types.DISMISS_TEAM_ADMIN)
def make_team_admin_action(action_type, user, team):
    data = {'user_id': user.id, 'username': user.username,
            'team_id': team.id, 'team_name': team.team_name,
            'team_desc': team.team_desc}
    return data, [(TYPE_SITE, 0), (TYPE_TEAM, team.id)]


@action_creator(action_types.GRANT_APPLICATION_AUTH)
@action_creator(action_types.DISMISS_APPLICATION_AUTH)
def make_application_auth_action(action_type, user, application, authority):
    data = {'application_id': application.id,
            'application_name': application.application_name,
            'user_id': user.id, 'username': user.username,
            'authority': authority}
    return data, [(TYPE_SITE, 0),
                  (TYPE_APPLICATION, application.id),
                  (TYPE_TEAM, application.team.id)]


@action_creator(action_types.UPDATE_SERVICE)
@action_creator(action_types.DELETE_SERVICE)
@action_creator(action_types.UPDATE_SWITCH)
@action_creator(action_types.DELETE_SWITCH)
@action_creator(action_types.UPDATE_CONFIG)
@action_creator(action_types.DELETE_CONFIG)
def make_configuration_action(
        action_type, application_name, cluster_name, key,
        old_data=None, new_data=None):
    data = {'old': old_data, 'new': new_data}
    data = {'application_name': application_name,
            'cluster_name': cluster_name, 'key': key, 'data': data}
    indices = itertools.chain(
        _optional_indices(application_name),
        _optional_instance_indices(
            application_name, cluster_name, key, action_type)
    )
    return data, list(indices)


@action_creator(action_types.IMPORT_SERVICE)
@action_creator(action_types.IMPORT_SWITCH)
@action_creator(action_types.IMPORT_CONFIG)
def make_import_action(action_type, datalist, overwrite, affected):
    nested = {}
    for item in datalist:
        _application = nested.setdefault(item['application'], {})
        _cluster = _application.setdefault(item['cluster'], {})
        _cluster[item['key']] = item['value']
    application_names = [application_name for application_name in nested]
    data = {'data': {'nested': nested}, 'stored': True,
            'overwrite': overwrite, 'affected': affected,
            'application_names': application_names}
    indices = itertools.chain.from_iterable(
        _optional_indices(application_name)
        for application_name in application_names)
    return data, list(indices)


@action_creator(action_types.UPDATE_INFRA_CONFIG)
@action_creator(action_types.DELETE_INFRA_CONFIG)
def make_infra_config_action(action_type, application_name, infra_type,
                             infra_name, scope_type, scope_name,
                             old_value=None, new_value=None):
    data = {'old': old_value, 'new': new_value}
    data = {'application_name': application_name, 'infra_type': infra_type,
            'infra_name': infra_name, 'scope_type': scope_type,
            'scope_name': scope_name, 'value': new_value, 'data': data}
    indices = _optional_indices(application_name)
    return data, list(indices)


@action_creator(action_types.CREATE_SERVICE_CLUSTER)
@action_creator(action_types.DELETE_SERVICE_CLUSTER)
@action_creator(action_types.CREATE_SWITCH_CLUSTER)
@action_creator(action_types.DELETE_SWITCH_CLUSTER)
@action_creator(action_types.CREATE_CONFIG_CLUSTER)
@action_creator(action_types.DELETE_CONFIG_CLUSTER)
def make_cluster_action(action_type, application_name, cluster_name):
    data = {'application_name': application_name, 'cluster_name': cluster_name}
    indices = _optional_indices(application_name)
    return data, list(indices)


@action_creator(action_types.ASSIGN_CLUSTER_LINK)
@action_creator(action_types.DELETE_CLUSTER_LINK)
def make_cluster_link_action(action_type, application_name, cluster_name,
                             physical_name=None):
    data = {'application_name': application_name, 'cluster_name': cluster_name,
            'physical_name': physical_name}
    indices = _optional_indices(application_name)
    return data, list(indices)


@action_creator(action_types.UPDATE_SERVICE_INFO)
@action_creator(action_types.UPDATE_CLUSTER_INFO)
def make_service_info_action(action_type, application_name, cluster_name=None,
                             old_data=None, new_data=None):
    data = {'old': old_data, 'new': new_data}
    data = {'application_name': application_name,
            'cluster_name': cluster_name, 'data': data}
    indices = _optional_indices(application_name)
    return data, list(indices)


@action_creator(action_types.UPDATE_ROUTE)
@action_creator(action_types.DELETE_ROUTE)
def make_route_action(action_type, application_name, cluster_name,
                      dest_application_name, dest_cluster_name,
                      intent, **kwargs):
    data = {'application_name': application_name, 'cluster_name': cluster_name,
            'intent': intent, 'dest_application_name': dest_application_name,
            'dest_cluster_name': dest_cluster_name}
    indices = _optional_indices(application_name)
    return data, list(indices)


@action_creator(action_types.UPDATE_DEFAULT_ROUTE)
@action_creator(action_types.DELETE_DEFAULT_ROUTE)
def make_default_route_action(action_type, application_name, ezone, intent,
                              cluster_name=None):
    data = {'application_name': application_name, 'ezone': ezone,
            'intent': intent, 'cluster_name': cluster_name}
    indices = _optional_indices(application_name)
    return data, list(indices)


@action_creator(action_types.PROGRAM_UPDATE_ROUTE_STAGE)
def make_program_route_stage_action(action_type, application_name, old_stage,
                                    new_stage):
    data = {'application_name': application_name,
            'old_stage': old_stage, 'new_stage': new_stage}
    indices = itertools.chain(
        _optional_indices(application_name),
        _optional_indices(SELF_APPLICATION_NAME))
    return data, list(indices)


def _optional_indices(application_name):
    application = None
    team = None
    with _suppress_exception(application_name=application_name):
        application = Application.get_by_name(application_name)
    with _suppress_exception(application_name=application_name):
        team = application and application.team
    if application:
        yield (TYPE_APPLICATION, application.id)
    if team:
        yield (TYPE_TEAM, team.id)


def _optional_instance_indices(
        application_name, cluster_name, instance_key, action_type):
    _, action_name = action_types[action_type].split('_', 1)
    instance_type = INSTANCE_TYPE_MAP[action_name.lower()]
    application = None
    with _suppress_exception(application_name=application_name):
        application = Application.get_by_name(application_name)
    if application:
        yield (instance_type, application.id, cluster_name, instance_key)


@contextlib.contextmanager
def _suppress_exception(**kwargs):
    try:
        yield
    except Exception as e:
        logger.error('Failed to create audit index: %r %r', e, kwargs)
