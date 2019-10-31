from __future__ import absolute_import

from marshmallow import Schema, fields, validate

from huskar_api.extras.marshmallow import LocalDateTime
from .validates import user_name_validate


class UserSchema(Schema):
    id = fields.Integer()
    username = fields.String(required=True, validate=user_name_validate)
    email = fields.String(required=True, validate=validate.Email())
    is_active = fields.Boolean()
    is_admin = fields.Boolean()
    is_application = fields.Boolean()
    last_login = LocalDateTime()
    created_at = LocalDateTime()
    updated_at = LocalDateTime()

    # backward compatibility
    huskar_admin = fields.Boolean(attribute='is_admin')
