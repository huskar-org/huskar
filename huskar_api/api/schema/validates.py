from __future__ import absolute_import

from marshmallow import validate
from marshmallow.exceptions import ValidationError


application_name_validate = validate.Regexp(
    r'^[a-zA-Z0-9][a-zA-Z0-9_\-.]{0,126}[a-zA-Z0-9]$',
    error=(u'AppID({input}) should consist by most 128 characters '
           u'of numbers, lowercase letters and underscore.'),
)
team_name_validate = validate.Regexp(
    r'^[^_][a-zA-Z0-9_\-\.]{1,32}$',
    error=(u'Team({input}) should consist by most 32 characters of numbers, '
           u' letters and underscores.')
)
cluster_name_validate = validate.Regexp(
    r'^(?!^\.+$)([a-zA-Z0-9_\-.]{1,64})$',
    error=(
        u'Cluster({input}) should consist by most 64 characters of numbers, '
        u'letters and underscores, and not starts with dots.')
)
user_name_validate = validate.Regexp(
    r'^[a-zA-Z0-9_\-.]{1,128}$',
    error=(u'Username({input}) should consist by most 128 characters '
           u'of numbers, lowercase letters and underscore.'),
)


def key_name_validate(value):
    regex_validate = validate.Regexp(
        r'^(?!^\.+$)\S+$',
        error=u'Key({input}) should not starts with dots or contains CRLF.'
    )
    value = regex_validate(value)
    if not all(0x00 < ord(c) < 0x7F for c in unicode(value)):
        raise ValidationError(
            u'Key({}) contains unicode characters.'.format(value))


def value_validate(value):
    if not value:
        raise ValidationError(u"Value can't be a empty string.")
