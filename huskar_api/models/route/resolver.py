from __future__ import absolute_import

import logging

from huskar_api.models.catalog import ServiceInfo
from huskar_api.models.exceptions import MalformedDataError
from huskar_api import settings
from huskar_api.switch import SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS, switch
from .utils import make_route_key, try_to_extract_ezone


logger = logging.getLogger(__name__)


class ClusterResolver(object):
    """The cluster resolver which applys symlink and route.

    :param get_service_info: A callable object which returns an instance of
                             :class:`.ServiceInfo`.
    :param get_cluster_info: A callable object which accepts ``cluster_name``
                             and returns an instance of :class:`.ClusterInfo`.
    """

    def __init__(self, get_service_info, get_cluster_info):
        self.get_service_info = get_service_info
        self.get_cluster_info = get_cluster_info

    def resolve_via_default(self, cluster_name, intent=None):
        ezone = try_to_extract_ezone(cluster_name)
        try:
            service_info = self.get_service_info()
        except MalformedDataError as e:
            logger.warning('Failed to parse default route "%s"', e.info.path)
            return ServiceInfo.find_global_default_route(ezone, intent)
        else:
            return service_info.find_default_route(ezone, intent)

    def resolve(self, cluster_name, from_application_name=None, intent=None,
                force_route_cluster_name=None):
        """Resolves the cluster name and returns the name of physical cluster.

        There are two steps to resolve a cluster. First, the route table will
        be checked if ``from_application_name`` is provided. Then, the resolved
        cluster will be resolved again, but uses the symlink configuration.

        There is an optional ``intent`` parameter which indicates the route
        intent in the first step, once ``from_application_name`` is provided.

        :param cluster_name: The original cluster name.
        :param from_application_name: Optional. The name of caller application.
        :param intent: Optional. The route intent of caller.
        :param force_route_cluster_name: Optional. The name of caller cluster
        :returns: The physical cluster name or ``None``.
        """
        cluster_info = None
        resolved_name = cluster_name

        if from_application_name:
            resolve_via_default = False
            try:
                cluster_info = self.get_cluster_info(cluster_name)
            except MalformedDataError as e:
                logger.warning('Failed to parse route "%s"', e.info.path)
                resolve_via_default = True
            else:
                route = cluster_info.get_route()
                route_key = make_route_key(
                    from_application_name, intent)
                resolved_name = route.get(route_key)
                if resolved_name is None:
                    resolve_via_default = True
            if resolve_via_default:
                resolved_name = self.resolve_via_default(cluster_name, intent)

        if resolved_name:
            try:
                if not cluster_info or resolved_name != cluster_name:
                    cluster_info = self.get_cluster_info(resolved_name)
            except MalformedDataError as e:
                logger.warning('Failed to parse symlink "%s"', e.info.path)
            else:
                resolved_name = cluster_info.get_link() or resolved_name

        if force_route_cluster_name in settings.FORCE_ROUTING_CLUSTERS and \
                switch.is_switched_on(SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS,
                                      default=False):
            cluster_key = _make_force_route_cluster_key(
                force_route_cluster_name, intent)
            # for route mode
            if (from_application_name and intent and
                    cluster_key in settings.FORCE_ROUTING_CLUSTERS):
                resolved_name = settings.FORCE_ROUTING_CLUSTERS.get(
                    cluster_key
                )
            else:
                # case: cluster_name is spec cluster which is dest cluster
                # ignore this cluster's link
                dest_clusters = settings.FORCE_ROUTING_CLUSTERS.values()
                if ((not from_application_name) and
                        cluster_name in dest_clusters):
                    resolved_name = cluster_name
                else:
                    resolved_name = settings.FORCE_ROUTING_CLUSTERS.get(
                        force_route_cluster_name
                    )

        if resolved_name != cluster_name:
            return resolved_name


def _make_force_route_cluster_key(cluster_name, intent):
    return '{}@{}'.format(cluster_name, intent)
