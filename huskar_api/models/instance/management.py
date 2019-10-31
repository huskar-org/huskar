from __future__ import absolute_import

import itertools

from kazoo.exceptions import NoNodeError
from huskar_sdk_v2.utils import encode_key, decode_key
from huskar_sdk_v2.consts import (
    SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN)

from huskar_api import settings
from huskar_api.models.catalog import ServiceInfo, ClusterInfo
from huskar_api.models.exceptions import NotEmptyError
from huskar_api.models.route import ClusterResolver
from .schema import Instance


class InstanceManagement(object):
    """The facade of configuration management.

    :param huskar_client: The instance of huskar client.
    :param application_name: The name of current application.
    :param type_name: The type of instance, which chould be ``service``,
                      ``switch`` or ``config``.
    """

    TYPE_NAME_CHOICES = (SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN)

    def __init__(self, huskar_client, application_name, type_name):
        assert type_name in self.TYPE_NAME_CHOICES
        self.huskar_client = huskar_client
        self.application_name = application_name
        self.type_name = type_name
        self.cluster_resolver = ClusterResolver(
            self.get_service_info, self.get_cluster_info)
        self.from_application_name = None
        self.from_cluster_name = None

    def set_context(self, from_application_name, from_cluster_name):
        """Sets the caller context for route.

        :param from_application_name: The name of caller application.
        :param from_cluster_name: The name of caller cluster.
        """
        self.from_application_name = from_application_name
        self.from_cluster_name = from_cluster_name

    def list_cluster_names(self):
        """Gets all cluster names of current application.

        :returns: The list of names.
        """
        info = self._make_service_info()
        try:
            return sorted(self.huskar_client.client.get_children(info.path))
        except NoNodeError:
            return []

    def list_instance_keys(self, cluster_name, resolve=True):
        """Gets all keys of specified cluster.

        The cluster will be resolved here, including the symlink and route.

        :param cluster_name: The name of callee cluster.
        :param resolve: ``False`` if you don't wanna resolving the cluster.
        :returns: The list of keys.
        """
        if resolve:
            physical_name = self.resolve_cluster_name(cluster_name)
            cluster_name = physical_name or cluster_name
        info = self._make_cluster_info(cluster_name)
        try:
            keys = sorted(self.huskar_client.client.get_children(info.path))
        except NoNodeError:
            keys = []
        return [decode_key(k) for k in keys]

    def get_service_info(self):
        """Gets the meta info of service.

        This method is implemented for **service** only.

        :raises MalformedDataError: The data source is malformed.
        :returns: The instance of :class:`ServiceInfo`.
        """
        assert self.type_name == SERVICE_SUBDOMAIN
        info = self._make_service_info()
        info.load()
        return info

    def get_cluster_info(self, cluster_name):
        """Gets the meta info of cluster.

        This method is implemented for **service** only.

        :param cluster_name: The name of cluster.
        :raises MalformedDataError: The data source is malformed.
        :returns: The instance of :class:`ClusterInfo`.
        """
        assert self.type_name == SERVICE_SUBDOMAIN
        info = self._make_cluster_info(cluster_name)
        info.load()
        return info

    def get_instance(self, cluster_name, key, resolve=True):
        """Gets the detail of instance.

        :param cluster_name: The name of cluster.
        :param key: The key of instance.
        :param resolve: ``False`` if you don't wanna resolving the cluster.
        :raises MalformedDataError: The data source is malformed. It happens
                                    only if :attr:`type_name` is **service**.
        :returns: The :class:`Instance` and its physical cluster name.
        """
        if resolve:
            physical_name = self.resolve_cluster_name(cluster_name)
            cluster_name = physical_name or cluster_name
        else:
            physical_name = None
        info = self._make_instance(cluster_name, key)
        info.load()
        return info, physical_name

    def resolve_cluster_name(self, cluster_name):
        """Resolves the cluster name and returns the name of physical cluster.

        See :class:`ClusterResolver` for details.

        :param cluster_name: The original cluster name, or the intent of route.
        :returns: The physical cluster name or ``None``.
        """
        if self.type_name != SERVICE_SUBDOMAIN:
            return
        if (self.from_application_name and self.from_cluster_name and
                cluster_name in settings.ROUTE_INTENT_LIST):
            physical_name = self.cluster_resolver.resolve(
                cluster_name=self.from_cluster_name,
                from_application_name=self.from_application_name,
                intent=cluster_name)
            return physical_name or self.from_cluster_name
        return self.cluster_resolver.resolve(cluster_name)

    def delete_cluster(self, cluster_name):
        """Deletes the cluster by its name.

        A cluster which could be deleted shall not include any instance or any
        using cluster info.

        :prama cluster_name: The name of cluster which will be deleted.
        :raises MalformedDataError: The data source is malformed.
        :raises NotEmptyError: if the cluster is not empty.
        :raises OutOfSyncError: if the operation need to be retried.
        :returns: ``None`` if the cluster does not exist, an instance of
                  :class:`ClusterInfo` if the cluster has been deleted.
        """
        service_info = self._make_service_info()
        service_info.load()

        cluster_info = self._make_cluster_info(cluster_name)
        cluster_info.load()

        if service_info.stat is None or cluster_info.stat is None:
            return

        outgoing_cluster_names = frozenset(
            itertools.chain.from_iterable(
                service_info.get_dependency().itervalues()
            )
        )

        if cluster_name in outgoing_cluster_names:
            raise NotEmptyError('There are outgoing routes in this cluster')
        if cluster_info.get_route() or cluster_info.get_link():
            raise NotEmptyError('There are routes or links in this cluster')
        if cluster_info.get_info():
            raise NotEmptyError('There are lb info in this cluster')
        if cluster_info.stat.children_count != 0:
            raise NotEmptyError('There are instances in this cluster')

        cluster_info.delete()
        return cluster_info

    def _make_service_info(self):
        return ServiceInfo(
            self.huskar_client.client,
            type_name=self.type_name,
            application_name=self.application_name)

    def _make_cluster_info(self, cluster_name):
        return ClusterInfo(
            self.huskar_client.client,
            type_name=self.type_name,
            application_name=self.application_name,
            cluster_name=cluster_name)

    def _make_instance(self, cluster_name, key):
        return Instance(
            self.huskar_client.client,
            type_name=self.type_name,
            application_name=self.application_name,
            cluster_name=cluster_name,
            key=encode_key(key))
