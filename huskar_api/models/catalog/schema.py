from __future__ import absolute_import

from marshmallow import Schema, fields, pre_load

from huskar_api.models.znode import ZnodeModel
from huskar_api.models.exceptions import MalformedDataError
from .route import RouteSchemaMixin, RouteMixin
from .default_route import DefaultRouteSchemaMixin, DefaultRouteMixin
from .dependency import DependencySchemaMixin, DependencyMixin
from .info import InfoMixin, InfoSchemaMixin


class DataFixMixin(Schema):
    """Fix the exception of marshmallow while "None" is accepted."""

    @pre_load
    def process_none(self, data):
        if data is None:
            return {}
        return data


class ServiceInfoSchema(InfoSchemaMixin, DependencySchemaMixin,
                        DefaultRouteSchemaMixin, DataFixMixin, Schema):
    _version = fields.Constant('1', dump_only=True)


class ClusterInfoSchema(InfoSchemaMixin, RouteSchemaMixin,
                        DataFixMixin, Schema):
    _version = fields.Constant('1', dump_only=True)


class DummyFactoryMixin(object):
    @classmethod
    def make_dummy(cls, data, **kwargs):
        """Make a dummy instance to support read-only operations."""
        # TODO Could we find a better way?
        instance = cls(client=None, **kwargs)
        instance.load = None
        instance.save = None
        if data:
            try:
                instance.data, _ = cls.MARSHMALLOW_SCHEMA.loads(data)
            except cls._MALFORMED_DATA_EXCEPTIONS as e:
                raise MalformedDataError(instance, e)
        return instance


class ServiceInfo(InfoMixin, DependencyMixin, DefaultRouteMixin,
                  DummyFactoryMixin, ZnodeModel):
    """The application-level control info.

    :param type_name: The catalog type (service, switch or config).
    :param application_name: The application name (a.k.a appid).
    """

    PATH_PATTERN = u'/huskar/{type_name}/{application_name}'
    MARSHMALLOW_SCHEMA = ServiceInfoSchema(strict=True)


class ClusterInfo(InfoMixin, RouteMixin, DummyFactoryMixin, ZnodeModel):
    """The cluster-level control info.

    :param type_name: The catalog type (service, switch or config).
    :param application_name: The application name (a.k.a appid).
    :param cluster_name: The cluster name.
    """

    PATH_PATTERN = u'/huskar/{type_name}/{application_name}/{cluster_name}'
    MARSHMALLOW_SCHEMA = ClusterInfoSchema(strict=True)
