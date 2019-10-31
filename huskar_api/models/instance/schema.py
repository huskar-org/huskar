from __future__ import absolute_import

import collections

from marshmallow import Schema, fields, validate, validates
from marshmallow.exceptions import ValidationError
from huskar_sdk_v2.consts import CONFIG_SUBDOMAIN, OVERALL

from huskar_api import settings
from huskar_api.models.exceptions import InfraNameNotExistError
from huskar_api.models.znode import ZnodeModel
from huskar_api.models.const import INFRA_CONFIG_KEYS
from huskar_api.extras.marshmallow import NestedDict, Polymorphic, Url


class InstanceSchema(object):
    """The marshmallow alike schema for instance."""

    def dumps(self, data):
        return data.encode('utf-8'), None

    def loads(self, data):
        return data.decode('utf-8'), None


class Instance(ZnodeModel):
    PATH_PATTERN = \
        u'/huskar/{type_name}/{application_name}/{cluster_name}/{key}'
    MARSHMALLOW_SCHEMA = InstanceSchema()


class InfraDatabaseSchema(Schema):
    URL_SCHEMES = {u'mysql', u'sam+mysql', u'pgsql', u'sam+pgsql'}

    master = fields.String(validate=Url(schemes=URL_SCHEMES), required=True)
    slave = fields.String(validate=Url(schemes=URL_SCHEMES), required=True)
    max_pool_size = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    max_pool_overflow = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    jdbc_url_parameters = fields.String(
        attribute='jdbc.urlParameters', load_from='jdbc.urlParameters',
        allow_none=True)


class InfraRedisSchema(Schema):
    URL_SCHEMES = {u'redis', u'sam+redis'}

    url = fields.String(validate=Url(schemes=URL_SCHEMES), required=True)
    max_pool_size = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    connect_timeout_ms = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    test_on_borrow = fields.Boolean(
        attribute='jedis.testOnBorrow', load_from='jedis.testOnBorrow',
        default=False, allow_none=True)
    test_on_return = fields.Boolean(
        attribute='jedis.testOnReturn', load_from='jedis.testOnReturn',
        default=False, allow_none=True)


class InfraAmqpSchema(Schema):
    URL_SCHEMES = {u'amqp', u'sam+amqp'}

    url = fields.String(validate=Url(schemes=URL_SCHEMES), required=True)
    connection_pool_size = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    channel_pool_size = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    write_timeout = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    auto_recover = fields.Boolean(default=True, allow_none=True)
    heartbeat = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)


class InfraElasticSearchSchema(Schema):
    URL_SCHEMES = {u'http', u'sam+http', u'transport', u'sam+transport'}

    url = fields.String(validate=Url(schemes=URL_SCHEMES), required=True)


class InfraMongoSchema(Schema):
    URL_SCHEMES = {u'mongo', u'sam+mongo'}

    url = fields.String(validate=Url(schemes=URL_SCHEMES), required=True)


class InfraOssSchema(Schema):
    URL_SCHEMES = {u'http', u'sam+http'}

    url = fields.String(validate=Url(schemes=URL_SCHEMES), required=True)
    max_pool_size = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    connect_timeout_ms = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    idle_timeout_ms = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)
    max_error_retry = fields.Integer(
        validate=validate.Range(min=0), allow_none=True)


class InfraKafkaSchema(Schema):
    URL_SCHEMES = {u'kafka', u'sam+kafka'}

    url = fields.String(validate=Url(schemes=URL_SCHEMES), required=True)
    group_id = fields.String(
        attribute='group.id', load_from='group.id', allow_none=True)


class InfraSchema(Schema):
    # Spec http://example.com/design/infra_key.html

    CHOICES = {
        'database': fields.Nested(InfraDatabaseSchema),  # Final
        'redis': fields.Nested(InfraRedisSchema),        # Final
        'amqp': fields.Nested(InfraAmqpSchema),          # Final
        'es': fields.Nested(InfraElasticSearchSchema),   # Draft
        'mongo': fields.Nested(InfraMongoSchema),        # Draft
        'oss': fields.Nested(InfraOssSchema),            # Draft
        'kafka': fields.Nested(InfraKafkaSchema),        # Draft
    }

    idcs = NestedDict(NestedDict(Polymorphic(CHOICES)))
    clusters = NestedDict(NestedDict(Polymorphic(CHOICES)))

    @validates('idcs')
    def validate_idcs(self, value):
        for key in value:
            if (key not in settings.ROUTE_EZONE_LIST and
                    key not in settings.ROUTE_IDC_LIST):
                raise ValidationError('%s is not valid zone' % key)


class InfraInfo(ZnodeModel):
    PATH_PATTERN = Instance.PATH_PATTERN
    MARSHMALLOW_SCHEMA = None

    _SCOPE_TYPES = ('idcs', 'clusters')
    _INFRA_CONFIG_URL_ATTRS = collections.defaultdict(lambda: ['url'], {
        'database': ['master', 'slave'],
        'oss': ['url'],
        'amqp': ['url'],
    })

    def __init__(self, client, application_name, infra_type):
        super(InfraInfo, self).__init__(
            client, type_name=CONFIG_SUBDOMAIN,
            application_name=application_name, cluster_name=OVERALL,
            key=INFRA_CONFIG_KEYS[infra_type])
        self.MARSHMALLOW_SCHEMA = InfraSchema(
            strict=True, context={'polymorphic_type_name': infra_type})
        self._url_attrs = self._INFRA_CONFIG_URL_ATTRS[infra_type]

    def list_by_infra_name(self, infra_name):
        data = self.data or {}
        return sorted(
            (scope_type, scope_name, config[infra_name])
            for scope_type in self._SCOPE_TYPES
            for scope_name, config in data.get(scope_type, {}).iteritems()
            if infra_name in config)

    def get_by_name(self, infra_name, scope_type, scope_name):
        config = self.setdefault({}) \
                     .setdefault(scope_type, {}) \
                     .setdefault(scope_name, {})
        if infra_name not in config:
            return
        return dict(config[infra_name])

    def set_by_name(self, infra_name, scope_type, scope_name, value):
        self._check_scope_type_value(scope_type, value)
        config = self.setdefault({}) \
                     .setdefault(scope_type, {}) \
                     .setdefault(scope_name, {})
        config[infra_name] = dict(value)

    def update_by_name(self, infra_name, scope_type, scope_name, value):
        self._check_scope_type_value(scope_type, value)
        config = self.setdefault({}) \
                     .setdefault(scope_type, {}) \
                     .setdefault(scope_name, {})
        if infra_name not in config:
            raise InfraNameNotExistError("can't find %s" % infra_name)
        config[infra_name].update(value)

    def delete_by_name(self, infra_name, scope_type, scope_name):
        assert scope_type in self._SCOPE_TYPES
        config = self.setdefault({}) \
                     .setdefault(scope_type, {}) \
                     .setdefault(scope_name, {})
        config.pop(infra_name, None)

    def extract_urls(self, value, as_dict=False):
        if as_dict:
            return {
                attr: value[attr] for attr in self._url_attrs if attr in value}
        return [value[attr] for attr in self._url_attrs if attr in value]

    def _check_scope_type_value(self, scope_type, value):
        assert scope_type in self._SCOPE_TYPES
        if not value or not isinstance(value, dict):
            raise ValueError('value should be a truly dictionary')
