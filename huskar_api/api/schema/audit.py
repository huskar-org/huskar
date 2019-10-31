from __future__ import absolute_import

from marshmallow import Schema, fields

from huskar_api.extras.marshmallow import LocalDateTime
from .user import UserSchema


class AuditLogSchema(Schema):
    id = fields.Integer()
    user = fields.Nested(UserSchema)
    remote_addr = fields.String()
    action_name = fields.String()
    action_data = fields.String()
    created_at = LocalDateTime()
    rollback_to = fields.Nested('self', exclude=['rollback_to'])
