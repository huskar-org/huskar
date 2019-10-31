from __future__ import absolute_import

from contextlib import contextmanager
import logging
import socket

from sqlalchemy.exc import SQLAlchemyError
from redis.exceptions import RedisError

from huskar_api import settings
from huskar_api.extras.monitor import monitor_client
from huskar_api.switch import switch, SWITCH_ENABLE_MINIMAL_MODE
from huskar_api.models.manifest import application_manifest
from huskar_api.models.signals import session_load_user_failed
from huskar_api.models.const import MM_REASON_SWITCH, MM_REASON_AUTH
from huskar_api.service.admin.exc import NoAuthError
from .user import User


logger = logging.getLogger(__name__)


class SessionAuth(object):
    """The authentication of specific session."""

    def __init__(self, username):
        self._name = username
        self._user = None
        self._minimal_mode = False
        self._minimal_mode_reason = None

    def __nonzero__(self):
        return bool(self._name and (self._user or self._minimal_mode))

    def __repr__(self):
        return 'SessionAuth(%r)' % self._name

    @classmethod
    def from_token(cls, token):
        username = User.parse_username_in_token(settings.SECRET_KEY, token)
        return cls(username)

    @classmethod
    def from_user(cls, user):
        instance = cls(user.username)
        instance._user = user
        return instance

    def load_user(self, username=None):
        username = username or self._name
        if username is None:
            return
        if switch.is_switched_on(SWITCH_ENABLE_MINIMAL_MODE, False):
            self.enter_minimal_mode(MM_REASON_SWITCH)
            return
        try:
            self._user = User.get_by_name(username)
        except (SQLAlchemyError, RedisError, socket.error):
            logger.exception('Enter minimal mode')
            self.enter_minimal_mode(MM_REASON_AUTH)
            session_load_user_failed.send(self)

    def enter_minimal_mode(self, reason=None):
        if self._minimal_mode:
            return
        self._minimal_mode = True
        self._minimal_mode_reason = reason
        monitor_client.increment('minimal_mode.qps', 1)

    @property
    def is_minimal_mode(self):
        return self._minimal_mode

    @property
    def minimal_mode_reason(self):
        if not self._minimal_mode:
            return
        return self._minimal_mode_reason

    @property
    def id(self):
        return self._user.id

    @property
    def username(self):
        if self._user:
            return self._user.username
        else:
            return self._name

    @property
    def is_application(self):
        if self._user:
            return self._user.is_application
        else:
            return application_manifest.check_is_application(self._name)

    @property
    def is_admin(self):
        if self._user:
            return self._user.is_admin
        else:
            return self._name in settings.ADMIN_EMERGENCY_USER_LIST

    def require_admin(self, message='admin authority needed'):
        if self.is_admin:
            return
        raise NoAuthError(message)

    @contextmanager
    def switch_as(self, username):
        origin_username = self._name
        self.load_user(username)

        try:
            yield
        finally:
            self.load_user(origin_username)
