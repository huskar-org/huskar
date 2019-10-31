from __future__ import absolute_import

from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN

from huskar_api.models.const import ROUTE_DEFAULT_INTENT
from huskar_api.models.catalog import ServiceInfo, ClusterInfo
from huskar_api.models.exceptions import MalformedDataError, EmptyClusterError
from huskar_api.models.route.resolver import ClusterResolver
from huskar_api.models.route.utils import make_route_key, parse_route_key


class RouteManagement(object):
    """The facade of route management.

    :param huskar_client: The instance of huskar client.
    :param application_name: The name of source application.
    :param cluster_name: The name of source cluster.
    """

    def __init__(self, huskar_client, application_name, cluster_name):
        self.huskar_client = huskar_client
        self.application_name = application_name
        self.cluster_name = cluster_name

    def make_service_info(self):
        info = ServiceInfo(
            self.huskar_client.client, type_name=SERVICE_SUBDOMAIN,
            application_name=self.application_name)
        info.load()
        return info

    def make_cluster_info(self, application_name):
        info = ClusterInfo(
            self.huskar_client.client, type_name=SERVICE_SUBDOMAIN,
            application_name=application_name, cluster_name=self.cluster_name)
        info.load()
        return info

    def yield_route_pairs(self, application_name):
        info = self.make_cluster_info(application_name)
        route = info.get_route()
        for route_key, cluster_name in route.items():
            route_key = parse_route_key(route_key)
            if route_key.application_name == self.application_name:
                yield route_key, cluster_name

    def list_route(self):
        """Lists all route rules for this source application and cluster.

        :raises MalformedDataError: if any dirty data is present
        :returns: Yields ``(application_name, intent, cluster_name)`` tuples.
        """
        dependency = self.make_service_info().get_dependency()
        for application_name in dependency:
            if self.cluster_name not in dependency[application_name]:
                continue
            is_found = False
            for route_key, cluster_name in self.yield_route_pairs(
                    application_name):
                is_found = True
                yield application_name, route_key.intent, cluster_name
            if not is_found:
                yield application_name, ROUTE_DEFAULT_INTENT, None

    def set_route(self, application_name, cluster_name, intent=None):
        """Creates or updates a route rule.

        :param application_name: The name of destination application.
        :param cluster_name: The name of destination cluster.
        :param intent: The optional intent flag.
        """
        self._check_route_arguments(application_name, cluster_name)
        # Update the depended clusters of source application
        service_info = safe_call(self.make_service_info)
        service_info.add_dependency(application_name, self.cluster_name)
        service_info.save()
        # Update the route rule
        cluster_info = safe_call(self.make_cluster_info, application_name)
        cluster_info.set_route(
            make_route_key(self.application_name, intent), cluster_name)
        cluster_info.save()

    def discard_route(self, application_name, intent=None):
        """Deletes a route rule.

        :param application_name: The name of destination application.
        :param intent: The optional intent flag.
        """
        # Update the route rule
        cluster_info = safe_call(self.make_cluster_info, application_name)
        dest_cluster_name = cluster_info.discard_route(
            make_route_key(self.application_name, intent))
        cluster_info.save()
        # Update the depended clusters of source application
        is_still_depended = any(
            parse_route_key(key).application_name == self.application_name
            for key in cluster_info.get_route())
        if is_still_depended:
            return dest_cluster_name
        service_info = safe_call(self.make_service_info)
        service_info.discard_dependency(application_name, self.cluster_name)
        service_info.save()
        return dest_cluster_name

    def get_default_route(self):
        """Gets all default route policy of this application.

        :raises MalformedDataError: if any dirty data is present
        :returns: The default route dictionary.
        """
        service_info = self.make_service_info()
        return service_info.get_default_route()

    def set_default_route(self, ezone, intent, cluster_name):
        """Creates or updates a default route of this application.

        :param ezone: The ezone ID (e.g. alta1).
        :param intent: The optional intent flag (could be ``None``).
        :param cluster_name: The unprefixed destination cluster name.
        :returns: The default route dictionary since this change.
        """
        intent = intent or ROUTE_DEFAULT_INTENT
        service_info = safe_call(self.make_service_info)
        service_info.set_default_route(ezone, intent, cluster_name)
        service_info.save()
        return service_info.get_default_route()

    def discard_default_route(self, ezone, intent):
        """Deletes a default route policy of this application.

        :param ezone: The ezone ID (e.g. alta1).
        :param intent: The optional intent flag (could be ``None``).
        :returns: The default route dictionary since this change.
        """
        intent = intent or ROUTE_DEFAULT_INTENT
        service_info = safe_call(self.make_service_info)
        service_info.discard_default_route(ezone, intent)
        service_info.save()
        return service_info.get_default_route()

    def declare_upstream(self, application_names):
        """Declares specified application and cluster as upstream.

        A common situation to call this method is in a service discovery
        session::

            route_management.declare_upstream(["foo.test"])

        :param application_names: A name list of applications.
        """
        service_info = safe_call(self.make_service_info)
        snapshot = service_info.freeze_dependency()
        for application_name in application_names:
            service_info.add_dependency(application_name, self.cluster_name)
        if service_info.freeze_dependency() == snapshot:
            return
        service_info.save()

    def _check_route_arguments(self, application_name, cluster_name):
        if not self._is_empty_cluster(application_name, cluster_name):
            return
        raise EmptyClusterError(
            'The target cluster {cluster_name} is empty.'.format(
                cluster_name=cluster_name))

    def _is_empty_cluster(self, application_name, cluster_name):
        def get_cluster_info(cluster):
            info = ClusterInfo(
                self.huskar_client.client, type_name=SERVICE_SUBDOMAIN,
                application_name=application_name, cluster_name=cluster)
            info.load()
            return info

        cluster_resolver = ClusterResolver(None, get_cluster_info)
        cluster_name = cluster_resolver.resolve(cluster_name) or cluster_name
        info = safe_call(get_cluster_info, cluster_name)
        return info.stat is None or info.stat.children_count == 0


def safe_call(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except MalformedDataError as e:
        return e.info
