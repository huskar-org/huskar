from __future__ import absolute_import

from flask import (
    g, current_app, got_request_exception, request_finished,
    request_tearing_down)
from flask_babel import Babel
from doctor import HealthTester, Configs
from raven.contrib.flask import Sentry
from sqlalchemy.exc import SQLAlchemyError
from redis.exceptions import RedisError

from huskar_api import settings


__all__ = ['sentry', 'db_tester']


class EnhancedSentry(Sentry):

    def init_app(self, *args, **kwargs):
        super(EnhancedSentry, self).init_app(*args, **kwargs)
        if self.client:
            # Downgrade the logging level of Sentry
            self.client.error_logger.error = self.client.error_logger.warning


class DatabaseHealthTester(object):
    """The health tester of database which acts as a Flask extension."""

    STATE_KEY = 'huskar_api.db.tester'
    DOCTOR_ARGS = ('huskar_api', 'db')

    def __init__(self):
        self.configs = Configs({
            'HEALTH_MIN_RECOVERY_TIME': settings.MM_MIN_RECOVERY_TIME,
            'HEALTH_MAX_RECOVERY_TIME': settings.MM_MAX_RECOVERY_TIME,
            'HEALTH_THRESHOLD_SYS_EXC': settings.MM_THRESHOLD_DB_ERROR,
            'HEALTH_THRESHOLD_UNKWN_EXC': settings.MM_THRESHOLD_UNKNOWN_ERROR,
        })

    def init_app(self, app):
        assert self.STATE_KEY not in app.extensions
        app.extensions[self.STATE_KEY] = HealthTester(self.configs)
        app.before_request(self.before_request)
        request_finished.connect(self.handle_request_finished, app)
        request_tearing_down.connect(self.handle_request_tearing_down, app)
        got_request_exception.connect(self.handle_got_request_exception, app)

        from huskar_api.models.signals import session_load_user_failed
        session_load_user_failed.connect(self.handle_load_user_failed)

    @property
    def ok(self):
        return g._db_is_healthy

    def before_request(self):
        tester = current_app.extensions[self.STATE_KEY]
        g._db_is_healthy = tester.test(*self.DOCTOR_ARGS)

    def handle_request_tearing_down(self, sender, **extra):
        tester = current_app.extensions[self.STATE_KEY]
        tester.metrics.on_api_called(*self.DOCTOR_ARGS)

    def handle_request_finished(self, sender, response, **extra):
        if 'X-Minimal-Mode' in response.headers:
            return
        tester = current_app.extensions[self.STATE_KEY]
        tester.metrics.on_api_called_ok(*self.DOCTOR_ARGS)

    def handle_got_request_exception(self, sender, exception, **extra):
        tester = current_app.extensions[self.STATE_KEY]
        if isinstance(exception, (SQLAlchemyError, RedisError)):
            tester.metrics.on_api_called_sys_exc(*self.DOCTOR_ARGS)
        else:
            tester.metrics.on_api_called_unkwn_exc(*self.DOCTOR_ARGS)

    def handle_load_user_failed(self, sender, **extra):
        tester = current_app.extensions[self.STATE_KEY]
        tester.metrics.on_api_called_sys_exc(*self.DOCTOR_ARGS)


babel = Babel()
sentry = EnhancedSentry()
db_tester = DatabaseHealthTester()
