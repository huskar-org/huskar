from __future__ import absolute_import

from marshmallow import Schema, fields

from huskar_api.extras.marshmallow import LocalDateTime, NamedTuple


class InfraDownstreamSchema(Schema):
    id = fields.Integer()
    user_application_name = fields.String()
    user_infra_type = fields.String()
    user_infra_name = fields.String()
    user_scope_pair = NamedTuple(['type', 'name'])
    user_field_name = fields.String()
    version = fields.Integer()
    created_at = LocalDateTime()
    updated_at = LocalDateTime()
