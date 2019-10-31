from __future__ import absolute_import

from flask import request, abort
from flask.views import MethodView
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN

from huskar_api.models import huskar_client
from huskar_api.models.auth import Authority
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.exceptions import MalformedDataError, OutOfSyncError
from huskar_api.models.utils import retry
from huskar_api.service.admin.application_auth import check_application_auth
from huskar_api.service.utils import check_cluster_name
from .utils import login_required, api_response, audit_log


class BaseInfoView(MethodView):
    UPDATE_ACTION_TYPE = None

    @login_required
    def get(self, application_name, **kwargs):
        """Gets the service info.

        The response looks like::

            {
              "data": {
                "protocol": "MySQL"
              },
              "message": "",
              "status": "SUCCESS"
            }

        :param application_name: The name of application.
        :param cluster_name: Optional. The name of cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application or cluster does not exist.
        :status 200: The request is successful.
        """
        cluster_name = kwargs.get('cluster_name')
        if cluster_name:
            check_cluster_name(cluster_name, application_name)
        instance_management = InstanceManagement(
            huskar_client, application_name, SERVICE_SUBDOMAIN)
        try:
            info = self._load(instance_management, **kwargs)
        except MalformedDataError:
            abort(404, 'Invalid data found.')
        else:
            data = info.get_info()
            return api_response(data)

    @login_required
    @retry(OutOfSyncError, interval=1, max_retry=3)
    def put(self, application_name, **kwargs):
        """Updates all fields.

        The ``write`` authority is required. See :ref:`application_auth` also.

        :param application_name: The name of application.
        :param cluster_name: Optional. The name of cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header Content-Type: ``application/json``
        :status 400: The request body is invalid. See response for detail.
        :status 404: The application or cluster does not exist.
        :status 200: The request is successful.
        """
        check_application_auth(application_name, Authority.WRITE)
        cluster_name = kwargs.get('cluster_name')
        if cluster_name:
            check_cluster_name(cluster_name, application_name)

        new_data = self._validate_payload()

        instance_management = InstanceManagement(
            huskar_client, application_name, SERVICE_SUBDOMAIN)
        info = self._load_silently(instance_management, **kwargs)

        old_data = info.get_info()
        info.set_info(new_data)
        info.save()

        audit_log.emit(
            self.UPDATE_ACTION_TYPE, old_data=old_data, new_data=new_data,
            application_name=application_name, **kwargs)
        return api_response()

    @login_required
    @retry(OutOfSyncError, interval=1, max_retry=3)
    def delete(self, application_name, **kwargs):
        """Deletes all fields.

        The ``write`` authority is required. See :ref:`application_auth` also.

        :param application_name: The name of application.
        :param cluster_name: Optional. The name of cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header Content-Type: ``application/json``
        :status 200: The request is successful.
        """
        check_application_auth(application_name, Authority.WRITE)
        cluster_name = kwargs.get('cluster_name')
        if cluster_name:
            check_cluster_name(cluster_name, application_name)

        instance_management = InstanceManagement(
            huskar_client, application_name, SERVICE_SUBDOMAIN)
        info = self._load_silently(instance_management, **kwargs)
        old_data = info.get_info()
        info.set_info({})
        info.save()

        audit_log.emit(
            self.UPDATE_ACTION_TYPE, old_data=old_data, new_data={},
            application_name=application_name, **kwargs)
        return api_response()

    def _load_silently(self, instance_management, **kwargs):
        try:
            return self._load(instance_management, **kwargs)
        except MalformedDataError as e:
            return e.info

    def _validate_payload(self):
        data = request.get_json(force=True)
        if not isinstance(data, dict):
            abort(400, 'The payload must be an object.')
        return data


class ServiceInfoView(BaseInfoView):
    UPDATE_ACTION_TYPE = audit_log.types.UPDATE_SERVICE_INFO

    def _load(self, instance_management):
        return instance_management.get_service_info()


class ClusterInfoView(BaseInfoView):
    UPDATE_ACTION_TYPE = audit_log.types.UPDATE_CLUSTER_INFO

    def _load(self, instance_management, cluster_name):
        return instance_management.get_cluster_info(cluster_name)
