from __future__ import absolute_import

import logging

import raven

from huskar_api import settings
from huskar_api.switch import (
    switch, SWITCH_ENABLE_SENTRY_MESSAGE, SWITCH_ENABLE_SENTRY_EXCEPTION)


logger = logging.getLogger(__name__)

raven_client = raven.Client(
    dsn=settings.SENTRY_DSN) if settings.SENTRY_DSN else None


def capture_message(*args, **kwargs):
    if not switch.is_switched_on(SWITCH_ENABLE_SENTRY_MESSAGE):
        return
    try:
        if raven_client:
            raven_client.captureMessage(*args, **kwargs)
        else:
            logger.warn('Ignored capture_message %r %r', args, kwargs)
    except Exception as e:
        logger.warn('Failed to send event to sentry: %r', e, exc_info=True)


def capture_exception(*args, **kwargs):
    if not switch.is_switched_on(SWITCH_ENABLE_SENTRY_EXCEPTION):
        return
    try:
        if raven_client:
            raven_client.captureException(*args, **kwargs)
        else:
            logger.warn('Ignored capture_exception with %r %r', args, kwargs)
    except Exception as e:
        logger.warn('Failed to send event to sentry: %r', e, exc_info=True)
