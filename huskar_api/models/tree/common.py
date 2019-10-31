from __future__ import absolute_import

import collections
import operator

from kazoo.recipe.cache import TreeCache
from huskar_sdk_v2.utils import combine

from huskar_api.models.const import ROUTE_LINKS_DELIMITER


class ClusterMap(object):
    """A bidirectional map of clusters."""

    __slots__ = ('resolved_names', 'cluster_names')

    def __init__(self):
        self.cluster_names = dict()
        self.resolved_names = collections.defaultdict(set)

    def register(self, cluster_name, resolved_name):
        assert cluster_name not in self.cluster_names
        if resolved_name is None:
            return
        self.cluster_names[cluster_name] = resolved_name
        for _resolved_name in resolved_name.split(ROUTE_LINKS_DELIMITER):
            self.resolved_names[_resolved_name].add(cluster_name)

    def deregister(self, cluster_name):
        previous_resolved_name = self.cluster_names.pop(cluster_name, None)
        if previous_resolved_name is None:
            return
        for _name in previous_resolved_name.split(ROUTE_LINKS_DELIMITER):
            self.resolved_names[_name].discard(cluster_name)


class Path(tuple):
    """The structured path."""

    #: ``service``, ``switch`` or ``config``
    type_name = property(operator.itemgetter(0))

    #: The name of application
    application_name = property(operator.itemgetter(1))

    #: The name of cluster
    cluster_name = property(operator.itemgetter(2))

    #: The key of instance
    data_name = property(operator.itemgetter(3))

    @classmethod
    def parse(cls, path, base_path=''):
        """Parses a string path and creates a structured path.

        :param path: The path of a ZooKeeper node.
        :param base_path: The base path such as ``/huskar``.
        :returns Path: A path tuple. For unknown path, none path will be
                       returned.
        """
        base_path = '{0}/'.format(base_path.rstrip('/'))
        if not path.startswith(base_path):
            return cls.make()
        components = path[len(base_path):].rstrip('/').split('/')
        if components == [''] or len(components) > 4:
            return cls.make()
        return cls.make(*components)

    @classmethod
    def make(cls, type_name=None, application_name=None, cluster_name=None,
             data_name=None):
        return cls((type_name, application_name, cluster_name, data_name))

    def get_level(self):
        """Gets the level of path."""
        return sum(1 for c in self[:4] if c is not None)

    def is_none(self):
        """Checks the path is none (invalid) or not."""
        return self.get_level() == 0

    def format(self, base_path):
        """Creates string path from the structured path."""
        return combine(base_path, *self[:4])


def parse_path(base_path, path):
    return Path.parse(path, base_path=base_path)


def make_path(base_path, *args, **kwargs):
    return Path.make(*args, **kwargs).format(base_path)


def make_cache(kazoo_client, path):
    return TreeCache(kazoo_client, path)
