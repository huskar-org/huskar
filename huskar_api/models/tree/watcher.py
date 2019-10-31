from __future__ import absolute_import

import collections
import logging
import time
import contextlib

from gevent import sleep
from gevent.queue import Queue
from kazoo.recipe.cache import TreeEvent
from huskar_sdk_v2.utils import decode_key
from huskar_sdk_v2.consts import (
    SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN)

from huskar_api import settings
from huskar_api.switch import (
    switch, SWITCH_DETECT_BAD_ROUTE, SWITCH_ENABLE_META_MESSAGE_CANARY)
from huskar_api.models.exceptions import TreeTimeoutError
from huskar_api.extras.monitor import monitor_client
from huskar_api.models.const import EXTRA_SUBDOMAIN_SERVICE_INFO
from .common import ClusterMap, parse_path, Path
from .extra import subdomain_map, extra_handlers


logger = logging.getLogger(__name__)


class TreeWatcher(object):
    """A watcher will subscribe events from a tree holder and turn them into
    iterator.

    :param tree_hub: A :class:`TreeHub` instance.
    :param from_application_name: The name of caller application.
    :param with_initial: ``True`` if you want to dump whole tree as the first
                         element of iterator.
    :param life_span: The life span in seconds of this session.
    """

    MESSAGE_TYPES = {
        TreeEvent.NODE_ADDED: 'update',
        TreeEvent.NODE_UPDATED: 'update',
        TreeEvent.NODE_REMOVED: 'delete'}

    TYPE_NAMES = (
        SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN,
        EXTRA_SUBDOMAIN_SERVICE_INFO)

    PATH_LEVEL_TYPE = 1         # /huskar/{type}
    PATH_LEVEL_APPLICATION = 2  # /huskar/{type}/{application}
    PATH_LEVEL_CLUSTER = 3      # /huskar/{type}/{application}/{cluster}
    PATH_LEVEL_INSTANCE = 4     # /huskar/{type}/{application}/{cluster}/{id}

    def __init__(self, tree_hub, from_application_name=None,
                 from_cluster_name=None, with_initial=False,
                 life_span=None, metrics_tag_from=None):
        self.hub = tree_hub

        # The optional route context
        self.from_application_name = from_application_name
        self.from_cluster_name = from_cluster_name

        self.with_initial = with_initial
        self.queue = Queue()
        self.holders = set()
        self.cluster_maps = collections.defaultdict(ClusterMap)
        self.cluster_whitelist = collections.defaultdict(set)
        self.watch_map = collections.defaultdict(set)
        self.life_span = life_span
        self._metrics_tag_from = metrics_tag_from

    def __iter__(self):
        """The tree watcher is iterable for subscribing events."""
        monitor_client.increment('tree_watcher.session', 1, tags={
            'from': str(self._metrics_tag_from),
            'appid': str(self._metrics_tag_from),
        })
        started_at = time.time()
        if self.with_initial:
            body = self._load_entire_body()
            yield ('all', body)
            monitor_client.increment('tree_watcher.event', 1, tags={
                'from': str(self._metrics_tag_from),
                'appid': str(self._metrics_tag_from),
                'event_type': 'all',
            })
        while True:
            while not self.queue.empty():
                event_type, body = self.queue.get()
                yield (event_type, body)
                monitor_client.increment('tree_watcher.event', 1, tags={
                    'from': str(self._metrics_tag_from),
                    'appid': str(self._metrics_tag_from),
                    'event_type': event_type,
                })
            yield ('ping', {})
            if self.life_span and time.time() > started_at + self.life_span:
                break
            sleep(1)

    def watch(self, application_name, type_name):
        """Watches a new subtree.

        :param application_name: The appid of subtree. (e.g. ``base.foo``)
        :param type_name: The type of subtree. (e.g. ``service``)
        """
        with self.maintain_watch_map(application_name, type_name) as type_name:
            holder = self.hub.get_tree_holder(application_name, type_name)
            if holder in self.holders:
                return
            try:
                holder.block_until_initialized(
                    timeout=settings.ZK_SETTINGS['treewatch_timeout'])
            except TreeTimeoutError:
                self.hub.release_tree_holder(application_name, type_name)
                raise

        cluster_map = self.cluster_maps[application_name, type_name]
        cluster_routes = holder.list_cluster_routes(
            self.from_application_name, self.from_cluster_name)
        for cluster_name, resolved_name in cluster_routes:
            cluster_map.register(cluster_name, resolved_name)

        self.holders.add(holder)
        holder.tree_changed.connect(self.handle_event, sender=holder)

    @contextlib.contextmanager
    def maintain_watch_map(self, application_name, type_name):
        subdomain = subdomain_map[type_name]
        self.watch_map[subdomain.name].add(application_name)
        try:
            yield subdomain.basic_name
        except Exception:
            self.watch_map[subdomain.name].discard(application_name)
            raise

    def limit_cluster_name(self, application_name, type_name, cluster_name):
        """Adds a whitelist item to limit events by cluster name.

        :param application_name: The appid of subtree. (e.g. ``base.foo``)
        :param type_name: The type of subtree. (e.g. ``service``)
        :param cluster_name: Only added cluster names will be shown.
        """
        self.cluster_whitelist[application_name, type_name].add(cluster_name)

    def handle_event(self, sender, event):
        path = parse_path(self.hub.base_path, event.event_data.path)
        path_level = path.get_level()

        if path.is_none() or path_level == self.PATH_LEVEL_TYPE:
            logger.warning('Unexpected path: %r', event)
            return

        # We should notify for changes of cluster route.
        if path_level in (
                self.PATH_LEVEL_APPLICATION,
                self.PATH_LEVEL_CLUSTER):

            # Publish message if and only if node is modified
            if (event.event_type == TreeEvent.NODE_ADDED and
                    not event.event_data.data):
                return

            cluster_map = self.cluster_maps[
                path.application_name, path.type_name]
            last_cluster_names = dict(cluster_map.cluster_names)
            self._update_cluster_route(path, event)

            # Publish message if and only if the callee cluster is watched
            if self._has_cluster_route_changed(path, last_cluster_names):
                # Dump all data for symlink or route changing
                entire_body = self._load_entire_body()
                message = ('all', entire_body)
                self.queue.put(message)
            else:
                # Dump updated data for watched extra types
                body = self.handle_event_for_extra_type('update', path)
                if body:
                    message = ('update', body)
                    self.queue.put(message)

        # We should notify for changes of instance node.
        if path_level == self.PATH_LEVEL_INSTANCE:
            data = event.event_data.data
            event_type = event.event_type
            if event_type == TreeEvent.NODE_REMOVED:
                data = None
            entire_body = self._dump_body([(path, data)])
            if entire_body:
                message = (self.MESSAGE_TYPES[event_type], entire_body)
                self.queue.put(message)
            return

    def _load_entire_body(self):
        entire_body = self._dump_body(self._iter_instance_nodes())
        extra_types_data = self.handle_all_for_extra_type()
        entire_body.update(extra_types_data)
        entire_body = self._fill_body(entire_body)
        return entire_body

    def _update_cluster_route(self, path, event):
        # symlink or route changed only at service scope
        path_level = path.get_level()
        cluster_map = self.cluster_maps[
            path.application_name, path.type_name]
        holder = self.hub.get_tree_holder(
            path.application_name, path.type_name)
        force_route_cluster_name = self.from_cluster_name \
            if path.type_name == SERVICE_SUBDOMAIN else None

        # Update cluster map for route
        if self.from_application_name and self.from_cluster_name:
            # NOTE It is not possible to know whether the changed cluster
            # is a middle node in the [route -> symlink -> physical] chain
            # style configuration.
            # We must resolve all intent in whichever cluster changed.
            for intent in settings.ROUTE_INTENT_LIST:
                resolved_name = holder.cluster_resolver.resolve(
                    self.from_cluster_name,
                    self.from_application_name,
                    intent,
                    force_route_cluster_name=force_route_cluster_name)
                if resolved_name is None:
                    resolved_name = self.from_cluster_name
                cluster_map.deregister(intent)
                cluster_map.register(intent, resolved_name)

        # Update cluster map for symlink
        if path_level == self.PATH_LEVEL_CLUSTER:
            resolved_name = holder.cluster_resolver.resolve(
                path.cluster_name,
                force_route_cluster_name=force_route_cluster_name)
            cluster_map.deregister(path.cluster_name)
            cluster_map.register(path.cluster_name, resolved_name)

    def _has_cluster_route_changed(self, path, last_cluster_names):
        cluster_map = self.cluster_maps[
            path.application_name, path.type_name]
        cluster_whitelist = self.cluster_whitelist[
            path.application_name, path.type_name]

        # Compare the difference of cluster names
        cluster_difference = set(dict(
            set(last_cluster_names.items()) ^
            set(cluster_map.cluster_names.items())))
        if cluster_difference:
            return not cluster_whitelist or (
                len(cluster_whitelist.intersection(cluster_difference)) != 0)
        return False

    def handle_all_for_extra_type(self):
        body = {}
        for type_name in subdomain_map.BASIC_SUBDOMAINS:
            path = Path.make(type_name=type_name)
            type_body = self.handle_event_for_extra_type('all', path)
            body.update(type_body)
        return body

    def handle_event_for_extra_type(self, event_type, path):
        body = {}
        if not switch.is_switched_on(SWITCH_ENABLE_META_MESSAGE_CANARY):
            return body
        extra_types = subdomain_map.get_extra_types(path.type_name)
        for extra_type in extra_types:
            type_data = body.setdefault(extra_type, {})
            application_names = set()
            if path.application_name:
                if path.application_name in self.watch_map[extra_type]:
                    application_names.add(path.application_name)
            else:
                application_names = self.watch_map[extra_type]
            for application_name in application_names:
                app_data = type_data.setdefault(application_name, {})
                handler = extra_handlers[extra_type, event_type]
                data = handler(self, Path.make(
                    path.type_name, application_name, path.cluster_name,
                    path.data_name))
                if data:
                    app_data.update(data)
        return {
            type_name: type_data
            for type_name, type_data in body.items()
            if any(type_data.values())}

    def _iter_instance_nodes(self):
        for holder in self.holders:
            if holder.type_name in self.watch_map:
                for path, data in holder.list_instance_nodes():
                    yield path, data

    def _dump_body(self, pairs):
        entire_body = {}
        for path, data in pairs:
            cluster_map = self.cluster_maps[
                path.application_name, path.type_name]
            cluster_whitelist = self.cluster_whitelist[
                path.application_name, path.type_name]
            cluster_names = set([path.cluster_name]).union(
                cluster_map.resolved_names[path.cluster_name])

            # We should ignore the subtree of callee cluster because it
            # has been overrided by symlink or route.
            if cluster_map.cluster_names.get(path.cluster_name):
                continue

            if cluster_whitelist:
                cluster_names = cluster_names & cluster_whitelist

            for cluster_name in cluster_names:
                data_body = entire_body \
                    .setdefault(path.type_name, {}) \
                    .setdefault(path.application_name, {}) \
                    .setdefault(cluster_name, {}) \
                    .setdefault(decode_key(path.data_name), {})
                data_body['value'] = data
        return entire_body

    def _fill_body(self, body):
        # Fills the type names and application names
        for type_name in self.TYPE_NAMES:
            type_data = body.setdefault(type_name, {})
            application_names = self.watch_map.get(type_name, [])
            for application_name in application_names:
                type_data.setdefault(application_name, {})
        # Fills the cluster names
        for (application_name, type_name), cluster_names \
                in self.cluster_whitelist.iteritems():
            for cluster_name in cluster_names:
                body.setdefault(type_name, {}) \
                    .setdefault(application_name, {}) \
                    .setdefault(cluster_name, {})
        # Checks extra information
        self._detect_bad_route(body)
        return body

    def _detect_bad_route(self, body):
        if not switch.is_switched_on(SWITCH_DETECT_BAD_ROUTE):
            return
        if self.from_application_name in settings.LEGACY_APPLICATION_LIST:
            return
        from_cluster_blacklist = settings.ROUTE_FROM_CLUSTER_BLACKLIST.get(
            self.from_application_name, [])
        if self.from_cluster_name in from_cluster_blacklist:
            return

        type_name = SERVICE_SUBDOMAIN
        type_body = body[type_name]

        flat_cluster_names = (
            (application_name, cluster_name, cluster_body)
            for application_name, application_body in type_body.iteritems()
            for cluster_name, cluster_body in application_body.iteritems())

        for application_name, cluster_name, cluster_body in flat_cluster_names:
            if application_name in settings.LEGACY_APPLICATION_LIST:
                continue
            if cluster_name in settings.ROUTE_DEST_CLUSTER_BLACKLIST.get(
                    application_name, []):
                continue

            cluster_map = self.cluster_maps[application_name, type_name]
            resolved_name = cluster_map.cluster_names.get(cluster_name)
            if cluster_body or not resolved_name:
                continue
            monitor_client.increment('tree_watcher.bad_route', 1, tags=dict(
                from_application_name=self.from_application_name,
                from_cluster_name=self.from_cluster_name,
                dest_application_name=application_name,
                appid=application_name,
                dest_cluster_name=cluster_name,
                dest_resolved_cluster_name=resolved_name,
            ))
            logger.info(
                'Bad route detected: %s %s %s %s -> %s (%r)',
                self.from_application_name, self.from_cluster_name,
                application_name, cluster_name, resolved_name,
                dict(cluster_map.cluster_names))
