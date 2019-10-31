from __future__ import absolute_import

import logging

from gevent.lock import Semaphore

from .holder import TreeHolder
from .watcher import TreeWatcher


logger = logging.getLogger(__name__)


class TreeHub(object):
    """The hub for holding multiple trees."""

    def __init__(self, huskar_client, startup_max_concurrency=None):
        self.base_path = huskar_client.base_path
        self.client = huskar_client.client
        self.tree_map = {}
        self.tree_holder_class = TreeHolder
        self.tree_watcher_class = TreeWatcher
        self.lock = Semaphore()
        if startup_max_concurrency:
            self.throttle = Semaphore(startup_max_concurrency)
        else:
            self.throttle = None

    def get_tree_holder(self, application_name, type_name):
        """Gets a tree holder which specified by its type and application.

        Example::

            holder = hub.get_tree_holder('switch', 'base.foo')

        If the tree holder does not exist, it will be created firstly.

        :returns: A :class:`TreeHolder` instance.
        """
        with self.lock:
            key = (application_name, type_name)
            if key not in self.tree_map:
                holder = self.tree_holder_class(
                    self, application_name, type_name, self.throttle)
                holder.start()
                self.tree_map[key] = holder
            return self.tree_map[key]

    def release_tree_holder(self, application_name, type_name):
        """Releases the tree holder.

        This method should be called after the
        :exc:`huskar_api.models.exceptions.TreeTimeoutError` raised.
        """
        holder = self.tree_map.pop((application_name, type_name), None)
        if holder is not None:
            holder.close()

        return holder

    def make_watcher(self, *args, **kwargs):
        """Creates a watcher and binds it to tree holders of this instance.

        :returns: A :class:`TreeWatcher` instance.
        """
        return self.tree_watcher_class(self, *args, **kwargs)
