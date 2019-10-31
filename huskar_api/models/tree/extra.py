from __future__ import absolute_import

from collections import namedtuple
from functools import wraps

from huskar_sdk_v2.consts import (
    SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN, OVERALL)

from huskar_api.models.const import EXTRA_SUBDOMAIN_SERVICE_INFO


Subdomain = namedtuple('Subdomain', ['name', 'basic_name'])


class DomainMap(object):
    BASIC_SUBDOMAINS = {
        SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN}

    def __init__(self, relations):
        assert set(relations.values()).issubset(self.BASIC_SUBDOMAINS)
        self._map = {
            name: Subdomain(name, basic_name)
            for name, basic_name in relations.items()}
        self._extras_map = {}
        for name, basic_name in relations.items():
            extras = self._extras_map.setdefault(basic_name, [])
            if name != basic_name:
                extras.append(name)

    def __getitem__(self, name):
        return self._map[name]

    def __contains__(self, name):
        return name in self._map

    def get_extra_types(self, basic_name):
        return self._extras_map[basic_name]


subdomain_map = DomainMap({
    EXTRA_SUBDOMAIN_SERVICE_INFO: SERVICE_SUBDOMAIN,
    SERVICE_SUBDOMAIN: SERVICE_SUBDOMAIN,
    CONFIG_SUBDOMAIN: CONFIG_SUBDOMAIN,
    SWITCH_SUBDOMAIN: SWITCH_SUBDOMAIN
})


class ExtraHandlerMap(object):
    EVENT_TYPES = ('update', 'all')

    def __init__(self):
        self._map = dict()

    def on_listen(self, subdomain, event_type):
        assert subdomain in subdomain_map
        assert event_type in self.EVENT_TYPES

        def decorator(handler):
            @wraps(handler)
            def wrapper(watcher, path):
                return handler(subdomain, watcher, path)
            self._map[subdomain, event_type] = wrapper
            return wrapper
        return decorator

    def __getitem__(self, topic):
        return self._map[topic]


extra_handlers = ExtraHandlerMap()


@extra_handlers.on_listen(EXTRA_SUBDOMAIN_SERVICE_INFO, 'update')
def handle_service_info_update(extra_type, watcher, path):
    """An update handler which can handle the events of ``PATH_LEVEL_CLUSTER``
    or `PATH_LEVEL_APPLICATION`, but treats the application event as the
    ``overall`` cluster event.

    Data that returned shoule be like::

        {
            "overall": {
                "balance_policy": '"RoundRobin"'
            }
        }

    :param extra_type: a extra subdomain type.
    :param watcher: an instance of ``Watcher``.
    :param path: an instance of ``Path``.
    :returns: None or data.
    """
    path_level = path.get_level()
    subdomain = subdomain_map[extra_type]
    if path_level not in (
            watcher.PATH_LEVEL_CLUSTER, watcher.PATH_LEVEL_APPLICATION):
        return {}

    cluster_whitelist = watcher.cluster_whitelist[
        path.application_name, subdomain.name]
    if path_level == watcher.PATH_LEVEL_APPLICATION:
        cluster_name = OVERALL
    else:
        cluster_name = path.cluster_name
    limited_clusters = {cluster_name}
    if cluster_whitelist and not limited_clusters.issubset(cluster_whitelist):
        return {}

    holder = watcher.hub.get_tree_holder(
        path.application_name, subdomain.basic_name)
    return {
        cluster_name: data or {}
        for cluster_name, data in holder.list_service_info(limited_clusters)
    }


@extra_handlers.on_listen(EXTRA_SUBDOMAIN_SERVICE_INFO, 'all')
def handle_service_info_all(extra_type, watcher, path):
    """An all handler which can only handles the events of
    ``PATH_LEVEL_APPLICATION``.

    Data that returned shoule be like::

        {
            "overall": {
                "balance_policy": '"RoundRobin"'
            },
            "stable": {
                "service_port": "8080"
            }
        }

    :param extra_type: a extra subdomain type.
    :param watcher: an instance of ``Watcher``.
    :param path: an instance of ``Path``.
    :returns: None or data.
    """
    path_level = path.get_level()
    if path_level != watcher.PATH_LEVEL_APPLICATION:
        return {}

    application_name = path.application_name
    subdomain = subdomain_map[extra_type]
    cluster_whitelist = watcher.cluster_whitelist[
        application_name, subdomain.name]
    holder = watcher.hub.get_tree_holder(
        application_name, subdomain.basic_name)
    return {
        cluster_name: info
        for cluster_name, info in holder.list_service_info(
            cluster_whitelist)
    }
