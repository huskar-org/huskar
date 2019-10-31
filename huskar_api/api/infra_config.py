from __future__ import absolute_import

import contextlib
import logging

from flask import request, abort
from flask.views import MethodView

from huskar_api import settings
from huskar_api.extras.email import EmailTemplate
from huskar_api.models import huskar_client
from huskar_api.models.auth import Authority, User, Application
from huskar_api.models.infra import (
    InfraDownstream, extract_application_names)
from huskar_api.models.instance import InfraInfo
from huskar_api.models.utils import retry
from huskar_api.models.const import INFRA_CONFIG_KEYS
from huskar_api.models.exceptions import OutOfSyncError
from huskar_api.service.admin.application_auth import (
    check_application, check_application_auth)
from .utils import (
    api_response, login_required, audit_log, deliver_email_safe)
from .schema import infra_downstream_schema


logger = logging.getLogger(__name__)


class InfraConfigView(MethodView):

    @login_required
    def get(self, application_name, infra_type, infra_name):
        """Gets the configuration of infrastructure in specified application.

        The response looks like::

            [{
              "scope_type": "idcs", "scope_name": "alta1",
              "value": {"url": "sam+redis://redis.foobar/overall"}
            }]

        :param application_name: The application which uses infrastructure.
        :param infra_type: ``database``, ``redis`` or ``amqp``.
        :param infra_name: The unique code-reference name of infrastructure.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The result is in the response.
        """
        check_application_auth(application_name, Authority.READ)
        check_infra_type(infra_type)
        infra_info = InfraInfo(
            huskar_client.client, application_name, infra_type)
        infra_info.load()
        infra_config = infra_info.list_by_infra_name(infra_name)
        return api_response({'infra_config': dump_infra_config(infra_config)})

    @login_required
    @retry(OutOfSyncError, interval=0.5, max_retry=3)
    def put(self, application_name, infra_type, infra_name):
        """Configures the infrastructure in specified application.

        The input schema looks like::

            {
              "url": "sam+redis://redis.foobar/overall"
            }

        :param application_name: The application which uses infrastructure.
        :param infra_type: ``database``, ``redis`` or ``amqp``.
        :param infra_name: The unique code-reference name of infrastructure.
        :query scope_type: ``idcs`` or ``clusters``.
        :query scope_name: The ezone id or cluster name.
        :query owner_mail: The email of resource owner which will receive the
                           notification of infra config creation.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header Content-Type: ``application/json``
        :status 200: The same as ``GET``.
        """
        with self._update(
                application_name, request, infra_type, infra_name) as args:
            application_name, infra_info, scope_type, scope_name, value = args
            owner_mail = request.args.get('owner_mail', '').strip()
            is_newcomer = len(infra_info.list_by_infra_name(infra_name)) == 0
            infra_info.set_by_name(infra_name, scope_type, scope_name, value)

        if is_newcomer and owner_mail:
            owned_application = Application.get_by_name(application_name)
            owner_user = User.get_by_email(owner_mail)
            infra_owner_emails = settings.ADMIN_INFRA_OWNER_EMAILS.get(
                infra_type, [])
            is_authorized = (
                owner_user is not None and
                owned_application.check_auth(Authority.READ, owner_user.id)
            )
            deliver_email_safe(EmailTemplate.INFRA_CONFIG_CREATE, owner_mail, {
                'application_name': application_name,
                'infra_name': infra_name,
                'infra_type': infra_type,
                'is_authorized': is_authorized,
            }, cc=infra_owner_emails)
        infra_config = infra_info.list_by_infra_name(infra_name)
        return api_response({'infra_config': dump_infra_config(infra_config)})

    @login_required
    @retry(OutOfSyncError, interval=0.5, max_retry=3)
    def patch(self, application_name, infra_type, infra_name):
        """Partially update existing infrastructures in specified application.

        The input schema looks like(for example for redis)::

            {
                "url": "sam+redis://redis.foobar/overall",
                "max_pool_size": 100,
                "connect_timeout_ms": 5,
                ...
            }

        :param application_name: The application which uses infrastructure.
        :param infra_type: ``database``, ``redis`` or ``amqp``.
        :param infra_name: The unique code-reference name of infrastructure.
        :query scope_type: ``idcs`` or ``clusters``.
        :query scope_name: The ezone id or cluster name.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header Content-Type: ``application/json``
        :status 200: The same as ``GET``.
        """
        with self._update(
                application_name, request, infra_type, infra_name) as args:
            application_name, infra_info, scope_type, scope_name, value = args
            scope_infra_content = infra_info.list_by_infra_name(infra_name)
            scope_content = (infra[:2] for infra in scope_infra_content)
            if (scope_type, scope_name) not in scope_content:
                abort(400, "%s doesn't exist" % infra_name)
            infra_info.update_by_name(
                infra_name, scope_type, scope_name, value)

        infra_config = infra_info.list_by_infra_name(infra_name)
        return api_response({'infra_config': dump_infra_config(infra_config)})

    @login_required
    @retry(OutOfSyncError, interval=0.5, max_retry=3)
    def delete(self, application_name, infra_type, infra_name):
        """Deletes the infrastructure configuration in specified application.

        :param application_name: The application which uses infrastructure.
        :param infra_type: ``database``, ``redis`` or ``amqp``.
        :param infra_name: The unique code-reference name of infrastructure.
        :query scope_type: ``idcs`` or ``clusters``.
        :query scope_name: The ezone id or clsuter name.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The same as ``GET``.
        """
        check_application_auth(application_name, Authority.WRITE)
        check_infra_type(infra_type)

        scope_type = request.args['scope_type']
        scope_name = request.args['scope_name']
        check_scope(scope_type, scope_name)

        infra_info = InfraInfo(
            huskar_client.client, application_name, infra_type)
        infra_info.load()
        old_value = infra_info.get_by_name(infra_name, scope_type, scope_name)
        infra_info.delete_by_name(infra_name, scope_type, scope_name)
        with audit_log(
                audit_log.types.DELETE_INFRA_CONFIG,
                application_name=application_name,
                infra_type=infra_type, infra_name=infra_name,
                scope_type=scope_type, scope_name=scope_name,
                old_value=old_value, new_value=None):
            infra_info.save()

        with suppress_exceptions('infra config deletion'):
            old_urls = infra_info.extract_urls(old_value or {}, as_dict=True)
            for field_name in frozenset(extract_application_names(old_urls)):
                InfraDownstream.unbind(
                    application_name, infra_type, infra_name, scope_type,
                    scope_name, field_name)

        infra_config = infra_info.list_by_infra_name(infra_name)
        return api_response({'infra_config': dump_infra_config(infra_config)})

    @contextlib.contextmanager
    def _update(self, application_name, request, infra_type, infra_name):
        check_application_auth(application_name, Authority.WRITE)
        check_infra_type(infra_type)

        scope_type = request.args['scope_type']
        scope_name = request.args['scope_name']
        check_scope(scope_type, scope_name)

        value = request.get_json()
        if not value or not isinstance(value, dict):
            abort(400, 'Unacceptable content type or content body')

        infra_info = InfraInfo(
            huskar_client.client, application_name, infra_type)
        infra_info.load()
        old_value = infra_info.get_by_name(infra_name, scope_type, scope_name)

        yield application_name, infra_info, scope_type, scope_name, value

        infra_urls = infra_info.extract_urls(value)
        infra_application_names = extract_application_names(infra_urls)
        new_value = infra_info.get_by_name(infra_name, scope_type, scope_name)

        with audit_log(audit_log.types.UPDATE_INFRA_CONFIG,
                       application_name=application_name,
                       infra_type=infra_type, infra_name=infra_name,
                       scope_type=scope_type, scope_name=scope_name,
                       old_value=old_value, new_value=new_value):
            for infra_application_name in infra_application_names:
                check_application(infra_application_name)

            infra_info.save()

            with suppress_exceptions('infra config updating'):
                infra_urls = infra_info.extract_urls(value, as_dict=True)
                infra_applications = extract_application_names(infra_urls)
                for field_name, infra_application_name in \
                        infra_applications.iteritems():
                    InfraDownstream.bind(
                        application_name, infra_type, infra_name, scope_type,
                        scope_name, field_name, infra_application_name)


class InfraConfigDownstreamView(MethodView):
    @login_required
    def get(self, infra_application_name):
        """Shows downstream information of infrastructure.

        The response looks like::

            [{
              "user_application_name": "base.foo",
              "user_infra_type": "database",
              "user_infra_name": "db-1",
              "version": 1000001,
              "updated_at": "... (ISO formatted datetime)",
              "created_at": "... (ISO formatted datetime)"
            }]

        :param infra_application_name: The application name of infrastructure.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The result is in the response.
        """
        r = InfraDownstream.get_multi_by_application(infra_application_name)
        data = infra_downstream_schema.dump(r, many=True).data
        return api_response({'downstream': data})

    @login_required
    def post(self, infra_application_name):
        """Invalidates cache and shows downstream information of infra.

        The response is the same to the ``GET`` method.

        :param infra_application_name: The application name of infrastructure.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The result is in the response.
        """
        InfraDownstream.flush_cache_by_application(infra_application_name)
        return self.get(infra_application_name)


def check_infra_type(infra_type):
    if infra_type not in INFRA_CONFIG_KEYS:
        abort(404, 'Specified infra_type is not found')


def check_scope(scope_type, scope_name):
    if scope_type == 'idcs':
        if (scope_name not in settings.ROUTE_EZONE_LIST and
                scope_name not in settings.ROUTE_IDC_LIST):
            abort(400, 'Unrecognized scope_name "%s"' % scope_name)
    elif scope_type == 'clusters':
        if not scope_name.strip():
            abort(400, 'Unrecognized scope_name "%s"' % scope_name)
    else:
        abort(400, 'Unrecognized scope_type "%s"' % scope_type)


def dump_infra_config(infra_config):
    return [
        {'scope_type': scope_type, 'scope_name': scope_name, 'value': value}
        for scope_type, scope_name, value in infra_config]


@contextlib.contextmanager
def suppress_exceptions(where):
    try:
        yield
    except Exception:
        logger.exception('Suppressed exception on %s', where)
