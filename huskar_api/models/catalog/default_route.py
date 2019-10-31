from __future__ import absolute_import

import copy

from marshmallow import Schema, fields
from huskar_sdk_v2.consts import OVERALL
from more_itertools import first

from huskar_api import settings
from huskar_api.models.znode import ZnodeModel
from huskar_api.models.const import ROUTE_DEFAULT_INTENT


class DefaultRouteSchemaMixin(Schema):
    default_route = fields.Dict()


class DefaultRouteMixin(ZnodeModel):
    def find_default_route(self, ezone, intent):
        intent = intent or ROUTE_DEFAULT_INTENT
        self.check_default_route_args(ezone, intent)
        data = self.data or {}
        default_route = data.get('default_route', {})

        # This value should be able to be found always. Because we demand the
        # ROUTE_DEFAULT_POLICY to cover all intent in the service_check script.
        # A ``ValueError`` will be raised if this premise is fake.
        cluster_name = first(
            route.get(intent) for route in (
                default_route.get(ezone, {}),
                default_route.get(OVERALL, {}),
                settings.ROUTE_DEFAULT_POLICY)
            if route.get(intent) is not None)
        return self._prefix_cluster_name(ezone, cluster_name)

    @classmethod
    def find_global_default_route(cls, ezone, intent):
        intent = intent or ROUTE_DEFAULT_INTENT
        cls.check_default_route_args(ezone, intent)
        cluster_name = settings.ROUTE_DEFAULT_POLICY[intent]
        return cls._prefix_cluster_name(ezone, cluster_name)

    def get_default_route(self):
        data = self.data or {}
        default_route = copy.deepcopy(data.get('default_route', {}))
        default_overall = default_route.setdefault(OVERALL, {})
        for intent, cluster_name in settings.ROUTE_DEFAULT_POLICY.iteritems():
            default_overall.setdefault(intent, cluster_name)
        return default_route

    def set_default_route(self, ezone, intent, cluster_name):
        if not cluster_name:
            raise ValueError('Unexpected empty cluster_name')
        self.check_default_route_args(ezone, intent, cluster_name)
        data = self.setdefault({})
        route = data.setdefault('default_route', {}).setdefault(ezone, {})
        route[intent] = cluster_name

    def discard_default_route(self, ezone, intent):
        self.check_default_route_args(ezone, intent)
        data = self.setdefault({})
        route = data.setdefault('default_route', {}).setdefault(ezone, {})
        route.pop(intent, None)

    @classmethod
    def check_default_route_args(cls, ezone, intent, cluster_name=u''):
        if ezone not in settings.ROUTE_EZONE_LIST and ezone != OVERALL:
            raise ValueError('Unexpected ezone')
        if intent not in settings.ROUTE_INTENT_LIST:
            raise ValueError('Unexpected intent')
        if any(cluster_name.startswith(x) for x in settings.ROUTE_EZONE_LIST):
            raise ValueError('Unexpected prefixed cluster_name')

    @classmethod
    def _prefix_cluster_name(cls, ezone, cluster_name):
        # It means that we meet a unknown ezone which in orig mode
        if ezone == OVERALL:
            return cluster_name
        return u'{0}-{1}'.format(ezone, cluster_name)
