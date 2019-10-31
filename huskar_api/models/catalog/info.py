from __future__ import absolute_import

from marshmallow import Schema, fields

from huskar_api.models.znode import ZnodeModel


class InfoSchemaMixin(Schema):
    info = fields.Dict()


class InfoMixin(ZnodeModel):

    def get_info(self):
        data = self.data or {}
        return data.get('info', {})

    def set_info(self, new_data):
        data = self.setdefault({})
        if new_data:
            data['info'] = new_data
        else:
            data.pop('info', None)
