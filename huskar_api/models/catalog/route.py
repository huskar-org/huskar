from __future__ import absolute_import

from marshmallow import Schema, fields

from huskar_api.models.znode import ZnodeModel
from huskar_api.models.const import ROUTE_LINKS_DELIMITER


class RouteSchemaMixin(Schema):
    link = fields.List(fields.String())
    route = fields.Dict()


class RouteMixin(ZnodeModel):
    def get_route(self):
        data = self.data or {}
        return data.get('route', {})

    def set_route(self, route_key, cluster_name):
        data = self.setdefault({})
        route = data.setdefault('route', {})
        route[route_key] = cluster_name

    def discard_route(self, route_key):
        data = self.setdefault({})
        route = data.setdefault('route', {})
        return route.pop(route_key, None)

    def get_link(self):
        data = self.data or {}
        link = data.get('link', [])
        return ROUTE_LINKS_DELIMITER.join(sorted(link)) or None

    def set_link(self, link):
        data = self.setdefault({})
        data['link'] = sorted(frozenset(link.split(ROUTE_LINKS_DELIMITER)))

    def delete_link(self):
        data = self.setdefault({})
        data['link'] = []
