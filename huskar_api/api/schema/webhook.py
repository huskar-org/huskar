from __future__ import absolute_import

from marshmallow import Schema, fields, validate
from huskar_api.models.audit import action_types
from huskar_api.models.webhook import Webhook


class WebhookSchema(Schema):
    webhook_url = fields.URL(required=True)
    webhook_type = fields.Integer(
        validate=validate.OneOf(
            Webhook.HOOK_TYPES,
            error='invalid webhook type'
        )
    )
    event_list = fields.List(
        fields.String(
            validate=validate.OneOf(
                action_types.action_map,
                error='not a valid event type'
            ),
        )
    )
