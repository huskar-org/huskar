from __future__ import absolute_import

from ipaddress import IPv4Address
from marshmallow import (
    Schema, fields, validate, validates, pre_load, post_load, ValidationError)
from marshmallow.utils import ensure_text_type

from huskar_api.extras.marshmallow import NestedDict


class ServiceInstanceValueSchema(Schema):
    ip = fields.String(
        required=True,
        error_messages={
            'required': 'ip must be provided in instance value'
        })
    port = NestedDict(
        fields.Integer(validate=validate.Range(min=1, max=65535)),
        required=True,
        error_messages={
            'required': 'port must be provided in instance value',
            'invalid': 'port must be a dict, eg: {{"port":{{"main": 8080}}'
        })
    state = fields.String(validate=validate.OneOf(
        ['up', 'down'], error='state must be "up" or "down".'))
    meta = fields.Dict(required=False)
    # TODO: remove these fields below
    idc = fields.String(required=False)
    cluster = fields.String(required=False)
    name = fields.String(required=False)

    @validates('ip')
    def validate_ip(self, value):
        try:
            IPv4Address(unicode(value))
        except ValueError:
            raise ValidationError('illegal IP address')

    @validates('port')
    def validate_port(self, value):
        if 'main' not in value:
            raise ValidationError('main port is required')

    @pre_load
    def ensure_meta_dict(self, data):
        data['meta'] = data.get('meta') or {}
        return data

    @post_load
    def coerce_meta_value(self, data):
        meta = data.setdefault('meta', {})
        for key, value in meta.iteritems():
            meta[key] = ensure_text_type(value or '')
        return data
