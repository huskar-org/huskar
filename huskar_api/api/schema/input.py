from __future__ import absolute_import

from marshmallow import Schema, fields, validates
from marshmallow.validate import Email
from .validates import application_name_validate, cluster_name_validate

_email_validator = Email()


class Clusters(Schema):
    application_name = fields.String(
        required=True,
        validate=application_name_validate,
    )
    clusters = fields.List(
        fields.String(
            required=True,
            validate=cluster_name_validate
        )
    )


class EventSubscribeSchema(Schema):
    service = fields.Dict()
    config = fields.Dict()
    switch = fields.Dict()
    service_info = fields.Dict()

    _clusters_schema = Clusters(strict=True)

    def _validate_clusters(self, value):
        for application_name, clusters in value.items():
            self._clusters_schema.validate(dict(
                application_name=application_name,
                clusters=clusters
            ))

    @validates('service')
    def validate_service(self, value):
        self._validate_clusters(value)

    @validates('config')
    def validate_config(self, value):
        self._validate_clusters(value)

    @validates('switch')
    def validate_switch(self, value):
        self._validate_clusters(value)


def validate_email(email):
    _email_validator(email)
