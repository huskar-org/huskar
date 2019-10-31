from __future__ import absolute_import

from marshmallow import Schema, fields, validates, ValidationError

from huskar_api.models.znode import ZnodeModel


class DependencySchemaMixin(Schema):
    dependency = fields.Dict()

    @validates('dependency')
    def validate_dependency(self, value):
        for application_name, cluster_names in value.iteritems():
            if (not isinstance(application_name, unicode) or
                    not application_name):
                raise ValidationError('Invalid application name')
            if not isinstance(cluster_names, list):
                raise ValidationError('Invalid cluster list')
            if not all(isinstance(n, unicode) for n in cluster_names):
                raise ValidationError('Invalid cluster name')


class DependencyMixin(ZnodeModel):
    def get_dependency(self):
        data = self.data or {}
        return data.get('dependency', {})

    def freeze_dependency(self):
        return _freeze_dependency(self.get_dependency())

    def add_dependency(self, application_name, cluster_name):
        dependency = self._initialize()
        cluster_names = set(dependency.get(application_name, []))
        cluster_names.add(cluster_name)
        dependency[application_name] = sorted(cluster_names)

    def discard_dependency(self, application_name, cluster_name):
        dependency = self._initialize()
        cluster_names = set(dependency.get(application_name, []))
        cluster_names.discard(cluster_name)
        dependency[application_name] = sorted(cluster_names)

    def _initialize(self):
        data = self.setdefault({})
        dependency = data.setdefault('dependency', {})
        return dependency


def _freeze_dependency(dependency):
    return frozenset((k, frozenset(v)) for k, v in dependency.iteritems() if v)
