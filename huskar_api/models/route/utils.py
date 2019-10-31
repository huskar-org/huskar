from __future__ import absolute_import

import collections

from huskar_sdk_v2.consts import OVERALL

from huskar_api import settings
from huskar_api.models.const import ROUTE_DEFAULT_INTENT

RouteKey = collections.namedtuple('RouteKey', 'application_name intent')


def make_route_key(application_name, intent=None):
    if intent and intent != ROUTE_DEFAULT_INTENT:
        return u'{0}@{1}'.format(application_name, intent)
    return application_name


def parse_route_key(route_key):
    args = route_key.split('@', 1)
    if len(args) == 1:
        return RouteKey(args[0], ROUTE_DEFAULT_INTENT)
    return RouteKey(*args)


def try_to_extract_ezone(cluster_name, default=OVERALL):
    for ezone in settings.ROUTE_EZONE_LIST:
        if cluster_name.startswith(u'{0}-'.format(ezone)):
            return ezone
    return default
