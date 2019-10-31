from __future__ import absolute_import

import logging

from huskar_sdk_v2.utils import encode_key

from huskar_api.models import huskar_client
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.exceptions import NotEmptyError
from .exc import DataNotExistsError, DataNotEmptyError
from .utils import check_cluster_name_in_creation


log = logging.getLogger(__name__)


# TODO [refactor] clean up the usage and re-design the api
class DataBase(object):
    """ ServiceBase for `Config`, `Switch` and `Service`. """

    client = None

    @classmethod
    def get_value(cls, application, cluster, key):
        return cls.client.get(
            application=application, cluster=cluster, key=encode_key(key))

    @classmethod
    def create(cls, application, cluster, key, value, version=None):
        cls.check_cluster_name_in_creation(application, cluster)
        cls.client.set(application=application,
                       cluster=cluster,
                       key=encode_key(key),
                       value=unicode(value).encode('utf-8'),
                       version=version)

    @classmethod
    def delete(cls, application, cluster, key, strict=False):
        cls.client.delete(application=application,
                          cluster=cluster,
                          key=encode_key(key),
                          strict=strict)

    @classmethod
    def create_cluster(cls, application, cluster, strict=False):
        cls.check_cluster_name_in_creation(application, cluster)
        cls.client.create_if_not_exist(application, cluster, strict)

    @classmethod
    def delete_cluster(cls, application, cluster, strict=False):
        data_type = cls.client.sub_domain
        im = InstanceManagement(huskar_client, application, data_type)
        try:
            cluster_info = im.delete_cluster(cluster)
        except NotEmptyError as e:
            if strict:
                raise DataNotEmptyError(*e.args)
        else:
            if cluster_info is None and strict:
                raise DataNotExistsError('cluster does not exist')

    @classmethod
    def check_cluster_name_in_creation(cls, application, cluster):
        if not cls.exists(application, cluster):
            check_cluster_name_in_creation(cluster, application)

    @classmethod
    def exists(cls, application, cluster, key=None):
        # TODO: Move this out of service module
        path = cls.client.get_path(application, cluster, key=key)
        return cls.client.raw_client.exists(path)
