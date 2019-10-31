from __future__ import absolute_import

import logging
import time

from kazoo.exceptions import NotEmptyError as KazooNotEmptyError, NoNodeError
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN

from huskar_api import settings
from huskar_api.extras.monitor import monitor_client
from huskar_api.models.utils import check_znode_path
from huskar_api.models.exceptions import NotEmptyError, ContainerUnboundError

logger = logging.getLogger(__name__)


class ContainerManagement(object):
    """The facade of container management.

    :param huskar_client: The instance of huskar client.
    :param container_id: The identity of managed container (a.k.a cid/task_id).
    """

    _ZPATH_CONTAINER = '/huskar/container/{cid}'
    _ZPATH_LOCATION = '/huskar/container/{cid}/{location}'
    _ZPATH_BARRIER_DIRECTORY = '/huskar/container-barrier'
    _ZPATH_BARRIER = '/huskar/container-barrier/{cid}'

    def __init__(self, huskar_client, container_id):
        self.huskar_client = huskar_client
        self.type_name = SERVICE_SUBDOMAIN
        self.container_id = container_id

    def _make_container_path(self):
        check_znode_path(self.container_id)
        path = self._ZPATH_CONTAINER.format(cid=self.container_id)
        return path

    def _make_location_path(self, location):
        check_znode_path(self.container_id, location)
        path = self._ZPATH_LOCATION.format(
            cid=self.container_id, location=location)
        return path

    def _make_barrier_path(self):
        check_znode_path(self.container_id)
        path = self._ZPATH_BARRIER.format(cid=self.container_id)
        return path

    def register_to(self, application_name, cluster_name):
        """Registers this container to a specific application and cluster.

        :param application_name: The name of registering application.
        :param cluster_name: The name of registering cluster.
        """
        location = _pack(application_name, cluster_name)
        path = self._make_location_path(location)
        self.huskar_client.client.ensure_path(path)
        monitor_client.increment('container.register')

    def deregister_from(self, application_name, cluster_name):
        """Deregisters a specific application and cluster on this container.

        :param application_name: The name of registering application.
        :param cluster_name: The name of registering cluster.
        """
        location = _pack(application_name, cluster_name)
        path = self._make_location_path(location)
        self.huskar_client.client.delete(path, recursive=True)
        monitor_client.increment('container.deregister')

    def lookup(self):
        """Lists all applications and clusters on this container.

        :returns: A list of ``(application_name, cluster_name)`` tuples.
        """
        path = self._make_container_path()
        try:
            location_list = self.huskar_client.client.get_children(path)
            return sorted(_unpack_list(location_list, self.container_id))
        except NoNodeError:
            return []

    def destroy(self):
        """Destroys this container at all.

        :raises NotEmptyError: The container is still used by some clusters.
                               You should lookup and deregister again.
        """
        path = self._make_container_path()
        try:
            self.huskar_client.client.delete(path)
        except NoNodeError:
            return
        except KazooNotEmptyError:
            raise NotEmptyError()
        else:
            monitor_client.increment('container.destroy')

    def set_barrier(self):
        """Sets a barrier to prevent future registering of this container.

        There is a configuration item, ``CONTAINER_BARRIER_LIFESPAN``, which
        decides the max lifespan of a barrier in seconds.
        """
        path = self._make_barrier_path()
        self.huskar_client.client.ensure_path(path)
        monitor_client.increment('container.barrier')

    def has_barrier(self):
        """Checks whether a registering barrier is here.

        There is a configuration item, ``CONTAINER_BARRIER_LIFESPAN``, which
        decides the max lifespan of a barrier in seconds.
        """
        path = self._make_barrier_path()
        stat = self.huskar_client.client.exists(path)
        return stat is not None and _barrier_is_still_alive(stat)

    def _unset_barrier(self):
        path = self._make_barrier_path()
        self.huskar_client.client.delete(path, recursive=True)

    @classmethod
    def vacuum_stale_barriers(cls, huskar_client):
        """Scans stale barriers and deletes them.

        This is an iterator factory. You need to use ``next`` to drive it
        running continually. If you just invoke this method and leave it alone,
        nothing will happen.

        The returned iterator deletes one stale barrier on its each step.

        :param huskar_client: The instance of huskar client.
        :returns: An iterator which generates ``(container_id, is_stale)``
        """
        directory_path = cls._ZPATH_BARRIER_DIRECTORY
        container_ids = huskar_client.client.get_children(directory_path)
        for container_id in sorted(container_ids):
            management = cls(huskar_client, container_id)
            if management.has_barrier():
                yield container_id, False
            else:
                management._unset_barrier()
                yield container_id, True

    def raise_for_unbound(self, application_name, cluster_name, key):
        """
        Deal with container has barrier
        :return:
        :raise: ContainerUnboundError: raise when container has barrier
        """
        if self.has_barrier():
            monitor_client.increment('container.barrier_deny', tags={
                'application_name': application_name,
                'appid': application_name,
                'cluster_name': cluster_name,
            })
            logger.info(
                'could not register service because of container barrier, '
                'application={}, cluster={}, key={}'.format(
                    application_name, cluster_name, key
                )
            )
            raise ContainerUnboundError()


def _barrier_is_still_alive(stat):
    barrier_age = time.time() - stat.last_modified
    return barrier_age <= settings.CONTAINER_BARRIER_LIFESPAN


_PACK_SEP = u'$'


def _pack(application_name, cluster_name):
    if _PACK_SEP in application_name or not application_name:
        raise ValueError(
                'Illegal application name({!r})'.format(application_name))
    if _PACK_SEP in cluster_name or not cluster_name:
        raise ValueError('Illegal cluster name({!r})'.format(cluster_name))
    return _PACK_SEP.join([application_name, cluster_name])


def _unpack(key):
    application_name, cluster_name = key.split(_PACK_SEP, 1)
    return application_name, cluster_name


def _unpack_list(key_list, container_id):
    for key in key_list:
        try:
            yield _unpack(key)
        except ValueError:
            logger.warning('Malformed container %r in %s', key, container_id)
