from __future__ import absolute_import

from huskar_api.models.comment import set_comment, get_comment


def save(key_type, application, cluster, key, comment=u''):
    set_comment(application, cluster, key_type, key, comment)


def delete(key_type, application, cluster, key):
    set_comment(application, cluster, key_type, key, None)


def get(key_type, application, cluster, key):
    return get_comment(application, cluster, key_type, key)
