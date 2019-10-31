from __future__ import absolute_import

import logging
import collections

from flask import json
from kazoo.exceptions import NoNodeError, BadVersionError, NodeExistsError
from huskar_sdk_v2.utils import encode_key
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN

from huskar_api.extras.payload import zk_payload
from huskar_api.models import huskar_client
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.exceptions import MalformedDataError, OutOfSyncError
from huskar_api.models.dataware.zookeeper import service_client
from .exc import ServiceValueError, ServiceLinkError, ServiceLinkExisted
from .data import DataBase

logger = logging.getLogger(__name__)


class ServiceData(DataBase):
    client = service_client  # data cache
    info_class = collections.namedtuple('Info', 'data stat')

    @classmethod
    def save(cls, application, cluster, key, value=None, runtime=None,
             version=None):
        """Register a service instance.

        :param str application: The name of application (appid).
        :param str cluster: The name of cluster.
        :param str key: The fingerprint of service instance.
        :param dict value: The information of service instance.
        :param dict runtime: The overlay information of service instance.
        :param int version: The version of service instance.
        """
        cls.check_cluster_name_in_creation(application, cluster)
        cluster_path = cls.client.get_path(application, cluster)
        service_path = cls.client.get_path(
            application, cluster, encode_key(key))

        raw_client = cls.client.raw_client
        raw_client.ensure_path(cluster_path)

        merged_value = {}
        merged_value.update(value or {})
        merged_value.update(runtime or {})

        try:
            remote_value, stat = raw_client.get(service_path)
        except NoNodeError:
            if not value:
                raise ServiceValueError(
                    '`value` should be provided while creating service.')
            json_merged_value = json.dumps(merged_value)
            try:
                raw_client.create(service_path, json_merged_value)
                zk_payload(payload_data=json_merged_value,
                           payload_type='create')
            except NodeExistsError as e:
                raise OutOfSyncError(e)
            stat = raw_client.exists(service_path)
            if stat is None:
                raise OutOfSyncError()
            return cls.info_class(merged_value, stat)
        else:
            if version is None:
                version = stat.version
            try:
                remote_value = json.loads(remote_value)
            except (ValueError, TypeError):
                logger.warning('Failed to parse %r', service_path)
                remote_value = {}
            new_value = dict(remote_value)
            new_value.update(value or {})
            new_value.update(runtime or {})
            json_new_value = json.dumps(new_value)
            try:
                stat = raw_client.set(
                    service_path, json_new_value, version=version)
                zk_payload(payload_data=json_new_value, payload_type='set')
            except BadVersionError as e:
                raise OutOfSyncError(e)
            return cls.info_class(new_value, stat)


class ServiceLink(object):
    @classmethod
    def get_link(cls, application_name, cluster_name):
        cluster_info = cls._get_cluster_info(application_name, cluster_name)
        return cluster_info.get_link()

    @classmethod
    def set_link(cls, application_name, cluster_name, link):
        im = InstanceManagement(
            huskar_client, application_name, SERVICE_SUBDOMAIN)

        if not im.list_instance_keys(link, resolve=False):
            raise ServiceLinkError('the target cluster is empty.')

        present_link_generator = (
            im.resolve_cluster_name(c) == cluster_name
            for c in im.list_cluster_names())
        if any(present_link_generator):
            raise ServiceLinkError((
                '{} has been linked, cluster can only be '
                'linked once').format(cluster_name))

        if im.resolve_cluster_name(link):
            raise ServiceLinkError((
                'there is a link under {}, cluster can only be '
                'linked once').format(link))

        if not cls._set_link(application_name, cluster_name, link):
            raise ServiceLinkExisted(
                '{} is already linked to {}'.format(cluster_name, link))

    @classmethod
    def delete_link(cls, application_name, cluster_name):
        cluster_info = cls._get_cluster_info(application_name, cluster_name)
        cluster_info.delete_link()
        cluster_info.save()

    @classmethod
    def _set_link(cls, application_name, cluster_name, link):
        cluster_info = cls._get_cluster_info(application_name, cluster_name)
        if cluster_info.get_link() != link:
            cluster_info.set_link(link)
            cluster_info.save()
            return True
        return False

    @classmethod
    def _get_cluster_info(self, application_name, cluster_name):
        im = InstanceManagement(
            huskar_client, application_name, SERVICE_SUBDOMAIN)
        try:
            return im.get_cluster_info(cluster_name)
        except MalformedDataError as e:
            logger.warning('Failed to parse symlink "%s"', e.info.path)
            return e.info
