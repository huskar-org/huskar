from __future__ import absolute_import

import logging

from flask import g

from huskar_api import settings
from huskar_api.models.auth import Application, Authority
from huskar_api.service.organization.exc import ApplicationNotExistedError
from .exc import NoAuthError


logger = logging.getLogger(__name__)


def check_application_auth(application_name, authority):
    assert authority in Authority

    if g.auth.is_minimal_mode:
        check_application(application_name)
        if authority == Authority.READ:
            return True
        if g.auth.is_admin or g.auth.is_application:
            return True
    else:
        application = check_application(application_name)
        if application.check_auth(authority, g.auth.id):
            return True
        if (application.domain_name in settings.AUTH_PUBLIC_DOMAIN and
                authority == Authority.READ):
            return True

    raise NoAuthError('{} has no {} authority on {}'.format(
        g.auth.username, authority.value, application_name))


def check_application(application_name):
    if is_application_blacklisted(application_name):
        raise ApplicationNotExistedError(
            'application: {} is blacklisted'.format(application_name))

    if g.auth.is_minimal_mode:
        return
    application = Application.get_by_name(application_name)
    if application is None:
        raise ApplicationNotExistedError(
            "application: {} doesn't exist".format(application_name))
    return application


def is_application_blacklisted(application_name):
    return application_name in settings.AUTH_APPLICATION_BLACKLIST


def is_application_deprecated(application_name):
    return application_name in settings.LEGACY_APPLICATION_LIST
