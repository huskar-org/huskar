from __future__ import absolute_import

from marshmallow import Schema, fields, validate
from .validates import (
    application_name_validate, cluster_name_validate,
    key_name_validate, value_validate)


class InstanceSchema(Schema):
    # The length of the application is not restricted before,
    # so we relaxed the check of the length
    application = fields.String(
        required=True,
        validate=application_name_validate)
    cluster = fields.String(required=True, validate=cluster_name_validate)
    key = fields.String(required=True, validate=key_name_validate)
    value = fields.String(required=True, validate=value_validate)
    comment = fields.String(validate.Length(max=2048))
