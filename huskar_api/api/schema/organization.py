from __future__ import absolute_import

import collections

from marshmallow import Schema, fields, pre_dump, post_dump

from huskar_api.models.route import lookup_route_stage
from huskar_api.service.admin.application_auth import (
    is_application_blacklisted, is_application_deprecated)
from .user import UserSchema
from .validates import application_name_validate, team_name_validate


class TeamSchema(Schema):
    name = fields.String(required=True, validate=team_name_validate)


class ApplicationSchema(Schema):
    name = fields.String(required=True, validate=application_name_validate)
    team_name = fields.String(required=True, validate=team_name_validate)
    team_desc = fields.String()
    route_stage = fields.Method('_route_stage')
    is_deprecated = fields.Method('_is_deprecated')
    is_blacklisted = fields.Method('_is_blacklisted')

    DataWrapper = collections.namedtuple('DataWrapper', [
        'route_stage_table', 'name', 'team_name', 'team_desc',
    ])

    @pre_dump(pass_many=True)
    def fill_route_stage(self, data, many):
        rs = lookup_route_stage()
        if many:
            return [self.DataWrapper(rs, **item) for item in data]
        return self.DataWrapper(rs, **data)

    @post_dump(pass_many=True)
    def strip_blacklisted_items(self, data, many):
        if many:
            return [item for item in data if not item['is_blacklisted']]
        return data

    def _route_stage(self, obj):
        return obj.route_stage_table.get(obj.name, {})

    def _is_deprecated(self, obj):
        return is_application_deprecated(obj.name)

    def _is_blacklisted(self, obj):
        return is_application_blacklisted(obj.name)


class ApplicationAuthSchema(Schema):
    authority = fields.String()
    user = fields.Nested(UserSchema)
    username = fields.FormattedString('{user.username}')
