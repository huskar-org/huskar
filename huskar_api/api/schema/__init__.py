from __future__ import absolute_import

from marshmallow import ValidationError

from huskar_api.switch import switch, SWITCH_VALIDATE_SCHEMA
from .user import UserSchema
from .instance import InstanceSchema
from .audit import AuditLogSchema
from .input import EventSubscribeSchema, validate_email
from .organization import ApplicationSchema, TeamSchema, ApplicationAuthSchema
from .infra import InfraDownstreamSchema
from .service import ServiceInstanceValueSchema
from .webhook import WebhookSchema


__all__ = ['user_schema', 'instance_schema', 'audit_log_schema',
           'application_schema', 'team_schema', 'service_value_schema',
           'event_subscribe_schema', 'validate_email', 'validate_fields',
           'webhook_schema']

user_schema = UserSchema(strict=True)
instance_schema = InstanceSchema(strict=True)
audit_log_schema = AuditLogSchema(strict=True)
application_schema = ApplicationSchema(strict=True)
application_auth_schema = ApplicationAuthSchema(strict=True)
team_schema = TeamSchema(strict=True)
event_subscribe_schema = EventSubscribeSchema(strict=True)
service_value_schema = ServiceInstanceValueSchema(strict=True)
webhook_schema = WebhookSchema(strict=True)

infra_downstream_schema = InfraDownstreamSchema(strict=True)


def validate_fields(schema, data, optional_fields=(), partial=True):
    """validate fields value but which field name in `optional_fields`
    and the value is None.
    """
    if not switch.is_switched_on(SWITCH_VALIDATE_SCHEMA, True):
        return

    fields = set(data)
    if not fields.issubset(schema.fields):
        raise ValidationError(
            'The set of fields "%s" is not a subset of %s'
            % (fields, schema))

    data = {k: v for k, v in data.items()
            if not (k in optional_fields and v is None)}
    schema.validate(data, partial=partial)
