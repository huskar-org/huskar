# coding: utf-8

from __future__ import absolute_import

import logging

from enum import Enum
from flask import render_template

from huskar_api import settings
from huskar_api.extras.mail_client import AbstractMailClient

__all__ = ['EmailTemplate', 'EmailDeliveryError', 'deliver_email']

logger = logging.getLogger(__name__)


class EmailTemplate(Enum):
    DEBUG = ('email-debug.html', u'Huskar Debug', {'foo'})
    SIGNUP = (
        'email-signup.html', u'Huskar 用户创建', {'username', 'password'})
    PASSWORD_RESET = (
        'email-password-reset.html', u'Huskar 密码重置',
        {'username', 'token', 'expires_in'})
    PERMISSION_GRANT = (
        'email-permission-grant.html', u'Huskar 权限变更',
        {'username', 'application_name', 'authority'})
    PERMISSION_DISMISS = (
        'email-permission-dismiss.html', u'Huskar 权限变更',
        {'username', 'application_name', 'authority'})
    INFRA_CONFIG_CREATE = (
        'email-infra-config-create.html', u'Huskar 基础资源绑定通知',
        {'infra_name', 'infra_type', 'application_name', 'is_authorized'})


class EmailDeliveryError(Exception):
    pass


def render_email_template(template, **kwargs):
    template = EmailTemplate(template)
    filename, subject, _ = template.value
    context = dict(kwargs)
    context['title'] = subject
    context['core_config'] = {
        'env': settings.ENV,
    }
    context['settings'] = settings
    return render_template(template.value, **context)


def deliver_email(template, receiver, arguments, cc=None, client=None):
    cc = cc or []
    _, subject, required_arguments = template.value
    if set(arguments) != required_arguments:
        raise ValueError('Invalid arguments')

    if client and isinstance(client, AbstractMailClient):
        message = render_email_template(template, **arguments)
        client.deliver_email(receiver, subject, message, cc)
