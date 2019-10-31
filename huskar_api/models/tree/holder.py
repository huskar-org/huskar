from __future__ import absolute_import

import logging
import json

from blinker import Namespace
from gevent.event import Event
from gevent.lock import Semaphore
from kazoo.recipe.cache import TreeNode, TreeEvent
from huskar_sdk_v2.consts import OVERALL, SERVICE_SUBDOMAIN

from huskar_api import settings
from huskar_api.extras.monitor import monitor_client
from huskar_api.extras.raven import capture_exception
from huskar_api.models.route import ClusterResolver
from huskar_api.models.catalog import ServiceInfo, ClusterInfo
from huskar_api.models.exceptions import TreeTimeoutError, MalformedDataError
from .common import make_path, make_cache, parse_path


logger = logging.getLogger(__name__)
blinker = Namespace()


class TreeHolder(object):
    """The holder of a tree cache.

    A tree holder is related to a ``/huskar/<type>/<application>`` subtree in
    ZooKeeper. It will broadcast tree events to the subscribers.

    :param tree_hub: A :class:`TreeHub` instance.
    :param application_name: The name of application. (e.g. ``base.foo``)
    :param type_name: ``"config"``, ``switch"`` or ``"service"``.
    :param semaphore: Optional semaphore for throttle. We acquire it before
                      starting and release it after initialized (whether
                      success or failure).
    """

    tree_changed = blinker.signal('tree_changed')

    CONNECTIVE_EVENTS = {
        TreeEvent.CONNECTION_SUSPENDED: 'SUSPENDED',
        TreeEvent.CONNECTION_RECONNECTED: 'RECONNECTED',
        TreeEvent.CONNECTION_LOST: 'LOST',
    }

    def __init__(self, tree_hub, application_name, type_name, semaphore=None):
        assert type_name in ('service', 'switch', 'config')

        self.hub = tree_hub
        self.application_name = application_name
        self.type_name = type_name
        self.path = make_path(self.hub.base_path, type_name, application_name)
        self.cache = make_cache(self.hub.client, self.path)
        self.initialized = Event()
        self._started = False
        self._closed = False
        self.cluster_resolver = ClusterResolver(
            self.get_service_info, self.get_cluster_info)
        if semaphore is None:
            self.throttle_semaphore = Semaphore()
        else:
            self.throttle_semaphore = semaphore

    def start(self):
        """Starts to synchronize the cache and subscribes events."""
        self.throttle_semaphore.acquire()
        self.cache.listen(self.dispatch_signal)
        self.cache.listen_fault(self.record_errors)
        self.cache.start()
        self._started = True

    def close(self):
        # This is greenlet-safe as we know.
        if self._closed:
            return
        self._closed = True

        # The tree which was never initialized should be cleaned here.
        if self._started and not self.initialized.is_set():
            # The dispatch_signal method has similar responsibility so it
            # checks the _closed attribute firstly to avoid from potential
            # concurrency-related bugs.
            self.throttle_semaphore.release()

        # Now we could release the memory of tree nodes
        self.cache.close()

    def block_until_initialized(self, timeout):
        if self.initialized.wait(timeout):
            return
        outstanding_ops = self.cache._outstanding_ops
        self.close()
        monitor_client.increment('tree_holder.tree_timeout', 1, tags={
            'type_name': self.type_name,
            'application_name': self.application_name,
            'appid': self.application_name,
        })
        monitor_client.increment(
            'tree_holder.tree_timeout.outstanding', outstanding_ops, tags={
                'type_name': self.type_name,
                'application_name': self.application_name,
                'appid': self.application_name,
            })
        raise TreeTimeoutError(self.application_name, self.type_name)

    def get_data(self, *args, **kwargs):
        """Gets the data of specified znode from cached data."""
        path = make_path(self.hub.base_path, *args, **kwargs)
        node = self.cache.get_data(path)
        return node and node.data

    def get_children(self, *args, **kwargs):
        """Gets the children under specified znode from cached data."""
        path = make_path(self.hub.base_path, *args, **kwargs)
        return self.cache.get_children(path)

    def get_service_info(self):
        """Gets the meta info of service from cached data.

        This method is used by :class:`.ClusterResolver`.
        """
        data = self.get_data(self.type_name, self.application_name)
        return ServiceInfo.make_dummy(
            data=data, type_name=self.type_name,
            application_name=self.application_name)

    def get_cluster_info(self, cluster_name):
        """Gets the meta info of cluster from cached data.

        This method is used by :class:`.ClusterResolver`.
        """
        data = self.get_data(
            self.type_name, self.application_name, cluster_name)
        return ClusterInfo.make_dummy(
            data=data, type_name=self.type_name,
            application_name=self.application_name,
            cluster_name=cluster_name)

    def list_cluster_routes(self, from_application_name=None,
                            from_cluster_name=None):
        """Gets the route table between clusters.

        :param from_application_name: The name of caller application.
        :param from_cluster_name: The name of caller cluster.
        """
        cluster_names = self.get_children(
            self.type_name, self.application_name)
        force_route_cluster_name = \
            from_cluster_name if self.type_name == SERVICE_SUBDOMAIN else None

        for cluster_name in cluster_names:
            resolved_name = self.cluster_resolver.resolve(
                cluster_name,
                force_route_cluster_name=force_route_cluster_name
            )
            if resolved_name is not None:
                yield (cluster_name, resolved_name)

        if not from_application_name or not from_cluster_name:
            return

        for intent in settings.ROUTE_INTENT_LIST:
            resolved_name = self.cluster_resolver.resolve(
                from_cluster_name, from_application_name,
                intent, force_route_cluster_name=force_route_cluster_name
            )
            yield (intent, resolved_name or from_cluster_name)

    def list_instance_nodes(self):
        # TODO avoid to touch private members in future
        application_node = self.cache._root
        for cluster_node in application_node._children.values():
            for instance_node in cluster_node._children.values():
                if (instance_node._state != TreeNode.STATE_LIVE or
                        instance_node._data is None):
                    continue
                path = parse_path(self.hub.base_path, instance_node._path)
                data = instance_node._data.data
                yield path, data

    def list_service_info(self, cluster_whitelist=()):
        application_node = self.cache._root
        for cluster_node in application_node._children.values():
            path = parse_path(self.hub.base_path, cluster_node._path)
            if not cluster_whitelist or path.cluster_name in cluster_whitelist:
                info_data = self._get_service_info_data(path.cluster_name)
                yield path.cluster_name, info_data
        if not cluster_whitelist or OVERALL in cluster_whitelist:
            info_data = self._get_service_info_data()
            # The overall cluster represents the service level infos
            yield OVERALL, info_data

    def _get_service_info_data(self, cluster_name=None):
        try:
            if cluster_name:
                info = self.get_cluster_info(cluster_name)
            else:
                info = self.get_service_info()
        except MalformedDataError:
            data = {}
        else:
            # There is only load balance data here
            data = info.get_info()
        return {
            key: {'value': json.dumps(val)}
            for key, val in data.items()}

    def record_errors(self, error):
        logger.exception(error)
        capture_exception(data=None)

    def dispatch_signal(self, event):
        if not self.initialized.is_set():
            if event.event_type == TreeEvent.INITIALIZED:
                monitor_client.increment('tree_holder.events.initialized', 1)
                self.initialized.set()
                # If the tree holder is closing, the throttle semaphore should
                # be maintaining in the close method instead here.
                if not self._closed:
                    self.throttle_semaphore.release()
            return

        # It is possible to receive following events also. But we don't need
        # them for now.
        # - TreeEvent.CONNECTION_SUSPENDED
        # - TreeEvent.CONNECTION_RECONNECTED
        # - TreeEvent.CONNECTION_LOST
        if event.event_type in self.CONNECTIVE_EVENTS:
            event_name = self.CONNECTIVE_EVENTS[event.event_type]
            logger.info(
                'Connective event %s happened on %s', event_name, self.path)
            monitor_client.increment('tree_holder.events.connective', 1)
            return

        if event.event_type in (
                TreeEvent.NODE_ADDED,
                TreeEvent.NODE_UPDATED,
                TreeEvent.NODE_REMOVED):
            self.tree_changed.send(self, event=event)
            monitor_client.increment('tree_holder.events.node', 1)
            return
