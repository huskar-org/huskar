from __future__ import absolute_import

from flask import request, abort
from flask.views import MethodView

from huskar_api.service.admin.application_auth import (
    check_application_auth, check_application)
from huskar_api.service.service import ServiceLink
from huskar_api.service.utils import check_cluster_name
from huskar_api.models.auth import Authority
from .utils import login_required, api_response, audit_log


class ServiceLinkView(MethodView):
    @login_required
    def get(self, application_name, cluster_name):
        """Gets the link status of a cluster.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: :js:class:`null` or the name of target physical cluster.
        """
        check_application(application_name)
        check_cluster_name(cluster_name, application_name)
        link = ServiceLink.get_link(application_name, cluster_name)
        if link:
            return api_response(link)
        return api_response()

    @login_required
    def post(self, application_name, cluster_name):
        """Links a cluster to another physical cluster.

        The ``write`` authority is required. See :ref:`application_auth` also.

        Once you configure a cluster A to link to B, the service instances of
        A will be hidden, the service instances of B will appear in cluster A.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster.
        :form link: The name of target physical cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The target cluster is empty.
        :status 200: The request is successful.
        """
        check_application_auth(application_name, Authority.WRITE)
        check_cluster_name(cluster_name, application_name)
        link = request.form['link'].strip()

        if not link or link == cluster_name:
            abort(400, 'bad link parameter')

        with audit_log(audit_log.types.ASSIGN_CLUSTER_LINK,
                       application_name=application_name,
                       cluster_name=cluster_name,
                       physical_name=link):
            ServiceLink.set_link(application_name, cluster_name, link)

        return api_response()

    @login_required
    def put(self, application_name, cluster_name):  # pragma: no cover
        self.post(application_name, cluster_name)

    @login_required
    def delete(self, application_name, cluster_name):
        """Unlinks a cluster from certain physical cluster.

        The ``write`` authority is required. See :ref:`application_auth` also.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster.
        :form link: Optional. If you pass this, the cluster will be unlinked
                    if and only if the current target physical cluster is the
                    same as the value of this.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The request is successful.
        """
        check_application_auth(application_name, Authority.WRITE)
        check_cluster_name(cluster_name, application_name)
        now_link = ServiceLink.get_link(application_name, cluster_name)
        with audit_log(audit_log.types.DELETE_CLUSTER_LINK,
                       application_name=application_name,
                       cluster_name=cluster_name,
                       physical_name=now_link):
            ServiceLink.delete_link(application_name, cluster_name)
        return api_response()
