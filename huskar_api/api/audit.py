from __future__ import absolute_import

from flask import request, g, abort
from flask.views import MethodView

from huskar_api.models.exceptions import DataConflictError
from huskar_api.models.auth import Team, Application, Authority
from huskar_api.models.audit import AuditLog
from huskar_api.service.admin.application_auth import (
    check_application, check_application_auth)
from huskar_api.service.utils import check_cluster_name
from .utils import api_response, login_required, minimal_mode_incompatible, \
    strptime2date
from .schema import audit_log_schema


class AuditLogView(MethodView):
    def __init__(self, target_type):
        self.target_type = target_type

    @login_required
    @minimal_mode_incompatible
    def get(self, name=None):
        """Gets the list of audit log.

        :param name: The name of team, application and also.
        :query start: The offset of pagination. Default is ``0``.
        :query date: The date specified to search.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 403: You don't have required authority.
        :status 501: The server is in minimal mode.
        :status 200: The result is in the response.
                     (See :ref:`Audit Log Schema <audit_schema>`)
        """
        start = request.args.get('start', type=int, default=0)
        start = max(start, 0)
        date = request.args.get('date', type=strptime2date)
        target_id = self._find_target(name)
        can_view_sensitive_data = AuditLog.can_view_sensitive_data(
            g.auth.id, self.target_type, target_id)
        items = self._get_audit_logs(target_id, start, date)
        if not can_view_sensitive_data:
            items = [item.desensitize() for item in items]
        return api_response(audit_log_schema.dump(items, many=True).data)

    def _find_target(self, name):
        if self.target_type is AuditLog.TYPE_SITE:
            return 0

        if self.target_type is AuditLog.TYPE_TEAM:
            team = Team.get_by_name(name)
            if team is not None:
                return team.id

        if self.target_type is AuditLog.TYPE_APPLICATION:
            application = Application.get_by_name(name)
            if application is not None:
                return application.id

    def _get_audit_logs(self, target_id, start, date):
        if target_id is None:
            return []
        if date:
            audit_logs = AuditLog.get_multi_by_index_with_date(
                self.target_type, target_id, date)
        else:
            audit_logs = AuditLog.get_multi_by_index(
                self.target_type, target_id)
        return audit_logs[start:start + 100]


class AuditRollbackView(MethodView):

    @login_required
    @minimal_mode_incompatible
    def put(self, application_name, audit_id):
        check_application_auth(application_name, Authority.WRITE)

        audit = AuditLog.get(audit_id)
        if audit is None:
            abort(404, 'The audit log not existed.')
        if not audit.can_rollback:
            abort(400, 'The audit log can\'t be rollbacked.')

        try:
            audit.rollback(g.auth.id, request.remote_addr)
        except DataConflictError:
            abort(409, 'The audit log has conflict.')

        return api_response()


class AuditTimelineView(MethodView):

    def __init__(self, instance_type):
        self.instance_type = instance_type

    @login_required
    @minimal_mode_incompatible
    def get(self, application_name, cluster_name, key):
        """Get the audit logs of specified instance key.

        :param application_name: The name of application.
        :param cluster_name: The name of clsuter.
        :param key: The key of instance.
        :query date: The date specified to search, default is today.
        :query start: The offset of pagination. Default is ``0``.
        :>header Authorization: Huskar Token (See :ref:`token`)
        :status 403: You don't have required authority.
        :status 501: The server is in minimal mode.
        :status 200: The result is in the response.
                     (See :ref:`Audit Log Schema <audit_schema>`)
        """
        check_application(application_name)
        check_cluster_name(cluster_name, application_name)

        start = request.args.get('start', type=int, default=0)
        application = Application.get_by_name(application_name)
        can_view_sensitive_data = AuditLog.can_view_sensitive_data(
            g.auth.id, self.instance_type, application.id)
        items = AuditLog.get_multi_by_instance_index(
            self.instance_type, application.id, cluster_name, key)
        items = items[start:start + 20]
        if not can_view_sensitive_data:
            items = [item.desensitize() for item in items]
        return api_response(audit_log_schema.dump(items, many=True).data)
