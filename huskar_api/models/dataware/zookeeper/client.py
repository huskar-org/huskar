# coding: utf-8

# TODO Expecting a volunteer to delete this module totally

from __future__ import absolute_import

from huskar_sdk_v2.consts import BASE_PATH
from huskar_sdk_v2.utils import combine
from kazoo.exceptions import NoNodeError, NodeExistsError, BadVersionError

from huskar_api.extras.payload import zk_payload
from huskar_api.models import huskar_client
from huskar_api.models.exceptions import OutOfSyncError
from huskar_api.service.exc import DataExistsError


class HuskarClient(object):
    '''
    base operations on config/service/switch
    '''

    def __init__(self, sub_domain):
        self.sub_domain = sub_domain

    @property
    def raw_client(self):
        return huskar_client.client

    def get_path(self, application, cluster, key=None):
        return combine(BASE_PATH, self.sub_domain, application, cluster, key)

    def get(self, application=None, cluster=None, key=None):
        # TODO [refactor] those should be different functions
        if application and cluster and key:  # application+cluster+key
            path = self.get_path(application, cluster, key)
            try:
                value, _ = self.raw_client.get(path)
                return value
            except NoNodeError:
                return None
        else:  # pragma: no cover
            raise NotImplementedError()

    def set(self, application, cluster, key, value, version=None):
        value = str(value) if value else ''
        path = self.get_path(application, cluster, key)
        try:
            if version is None:
                self.raw_client.set(path, value)
            else:
                self.raw_client.set(path, value, version)
            zk_payload(payload_data=value, payload_type='set')
        except NoNodeError:
            self.raw_client.create(path, value, makepath=True)
            zk_payload(payload_data=value, payload_type='create')
        except BadVersionError as e:
            raise OutOfSyncError(e)

    def delete(self, application, cluster=None, key=None, strict=False):
        path = self.get_path(application, cluster, key)
        self.raw_client.delete(path, recursive=True)

    def create_if_not_exist(self, application, cluster=None, strict=False):
        path = self.get_path(application, cluster)
        try:
            self.raw_client.create(path, b'', makepath=True)
            zk_payload(payload_data=b'', payload_type='create')
        except NodeExistsError:
            if strict:
                target = 'application' if cluster is None else 'cluster'
                raise DataExistsError('%s exists already' % target)
