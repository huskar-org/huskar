from __future__ import absolute_import

import uuid
import datetime

from flask import abort
from werkzeug.security import safe_str_cmp

from huskar_api.models import DBSession, cache_manager
from huskar_api.models.auth import User
from huskar_api.extras.email import deliver_email, EmailTemplate


_PASSWORD_RESET_KEY = '%s:reset_password:{username}:token' % __name__
_PASSWORD_RESET_DURATION = datetime.timedelta(minutes=10)

_redis_client = cache_manager.make_client(namespace='%s:v1' % __name__)


# TODO deprecate
def request_to_reset_password(username):
    user = User.get_by_name(username)
    if not user or user.is_application:
        abort(404, u'user {0} not found'.format(username))
    if not user.email:
        abort(403, u'user {0} does not have email'.format(username))

    # Generate and record the token
    token = uuid.uuid4()
    _redis_client.set(
        raw_key=_PASSWORD_RESET_KEY.format(username=username),
        val=token.hex, expiration_time=_PASSWORD_RESET_DURATION)

    deliver_email(EmailTemplate.PASSWORD_RESET, user.email, {
        'username': user.username,
        'token': token,
        'expires_in': _PASSWORD_RESET_DURATION,
    })
    return user, token


# TODO deprecate
def reset_password(username, token, new_password):
    key = _PASSWORD_RESET_KEY.format(username=username)
    expected_token = _redis_client.get(key)
    if expected_token and safe_str_cmp(token.hex, expected_token):
        _redis_client.delete(key)
        user = User.get_by_name(username)
        if user is None or user.is_application:
            abort(404, u'user {0} not found'.format(username))
        user.change_password(new_password)
    else:
        abort(403, u'token is expired')
    return user


# TODO deprecate
def change_email(user, new_email):
    with DBSession().close_on_exit(False):
        user.email = new_email
