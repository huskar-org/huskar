from __future__ import absolute_import

import copy
import re

from marshmallow.fields import Field, DateTime, Dict
from marshmallow.validate import URL as _Url
from marshmallow.exceptions import ValidationError
from dateutil.tz import tzlocal


__all__ = ['LocalDateTime', 'NestedDict', 'Polymorphic']


class LocalDateTime(DateTime):
    localtime = True

    def _serialize(self, value, *args, **kwargs):
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=tzlocal())
        return super(LocalDateTime, self)._serialize(
            value, *args, **kwargs)


class NestedDict(Dict):
    def __init__(self, nested, *args, **kwargs):
        super(NestedDict, self).__init__(*args, **kwargs)
        self.nested = nested

    def __deepcopy__(self, memo):
        copied = super(NestedDict, self).__deepcopy__(memo)
        copied.nested = copy.deepcopy(self.nested)
        return copied

    def _deserialize(self, value, attr, data):
        value = super(NestedDict, self)._deserialize(value, attr, data)
        result = {}
        errors = {}
        for k, v in value.iteritems():
            try:
                result[k] = self.nested.deserialize(v, k, value)
            except ValidationError as e:
                result[k] = e.data
                errors[k] = e.messages
        if errors:
            raise ValidationError(errors, data=result)
        return result

    def _add_to_schema(self, field_name, schema):
        super(NestedDict, self)._add_to_schema(field_name, schema)
        self.nested._add_to_schema(field_name, self)


class Polymorphic(Field):
    def __init__(self, choices, *args, **kwargs):
        super(Polymorphic, self).__init__(*args, **kwargs)
        self.choices = choices

    def __deepcopy__(self, memo):
        copied = super(Polymorphic, self).__deepcopy__(memo)
        copied.choices = copy.deepcopy(self.choices)
        return copied

    def _deserialize(self, value, attr, data):
        choice_name = self.parent.context['polymorphic_type_name']
        choice = self.choices[choice_name]
        return choice.deserialize(value, attr, data)

    def _add_to_schema(self, field_name, schema):
        super(Polymorphic, self)._add_to_schema(field_name, schema)
        choice_name = schema.context['polymorphic_type_name']
        choice = self.choices[choice_name]
        choice._add_to_schema(field_name, self)


class Url(_Url):
    """The URL validator which is compatible with Sam."""

    # Copied and modified from marshmallow/validate.py
    URL_REGEX = re.compile(
        r'^(?:[a-z0-9\.\-\+]*)://'  # scheme is validated separately
        r'(?:[^:/]*(?::.*)?@)?'     # basic auth (RFC 1738)
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+'
        r'(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'(?:[a-zA-Z0-9][a-zA-Z0-9_\-.]{0,126}[a-zA-Z0-9])|'  # app_id (Sam)...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
        r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    def __init__(self, error=None, schemes=None):
        super(Url, self).__init__(relative=False, error=error, schemes=schemes)


class NamedTuple(Field):
    def __init__(self, fields, *args, **kwargs):
        super(NamedTuple, self).__init__(*args, **kwargs)
        self.fields = fields

    def _serialize(self, value, attr, obj):
        return dict(zip(self.fields, value))
