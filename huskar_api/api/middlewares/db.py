from __future__ import absolute_import

import logging

from flask import Blueprint

from huskar_api import settings
from huskar_api.ext import sentry
from huskar_api.models import DBSession


bp = Blueprint('middlewares.db', __name__)
logger = logging.getLogger(__name__)


@bp.after_app_request
def close_session(response):
    try:
        DBSession.remove()
    except Exception:
        logger.exception('Failed to close database during request teardown')
        sentry.captureException()
        if settings.TESTING:
            raise
    return response
