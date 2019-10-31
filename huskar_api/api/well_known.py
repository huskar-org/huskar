from __future__ import absolute_import

from flask.views import MethodView

from huskar_api import settings
from .utils import api_response


class WellKnownCommonView(MethodView):
    def get(self):
        """Gets the common well-known data.

         An example of response::

            {
              "status": "SUCCESS",
              "data": {
                "framework_versions": {
                  "latest": {
                  }
                },
                "idc_list": ["alta", "altb"],
                "ezone_list": ["alta1", "altb1"],
                "route_default_hijack_mode": {
                    "alta1": "S",
                    "altb1": "D",
                    "altc1": "S"
                },
                "force_routing_clusters": {
                }
              }
            }
        """
        route_default_hijack_mode = settings.ROUTE_EZONE_DEFAULT_HIJACK_MODE
        data = {
            'framework_versions': settings.FRAMEWORK_VERSIONS,
            'idc_list': settings.ROUTE_IDC_LIST,
            'ezone_list': settings.ROUTE_EZONE_LIST,
            'route_default_hijack_mode': route_default_hijack_mode,
            'force_routing_clusters': settings.FORCE_ROUTING_CLUSTERS,
        }
        return api_response(data)
