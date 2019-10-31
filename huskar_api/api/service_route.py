from __future__ import absolute_import

from flask import request, abort
from flask.views import MethodView
from huskar_sdk_v2.consts import OVERALL
from more_itertools import first

from huskar_api import settings
from huskar_api.models import huskar_client
from huskar_api.models.auth import Authority
from huskar_api.models.const import ROUTE_DEFAULT_INTENT
from huskar_api.models.exceptions import OutOfSyncError, EmptyClusterError
from huskar_api.models.route import RouteManagement
from huskar_api.models.utils import retry
from huskar_api.service.admin.application_auth import (
    check_application_auth, check_application)
from huskar_api.service.admin.exc import NoAuthError
from huskar_api.service.utils import check_cluster_name
from .utils import login_required, api_response, audit_log


class ServiceRouteView(MethodView):
    @login_required
    def get(self, application_name, cluster_name):
        """Gets the outgoing route of specific cluster.

        Example of response::

           {
             "status": "SUCCESS",
             "message": "",
             "data": {
               "route": [
                 {"application_name": "base.foo", "intent": "direct",
                  "cluster_name": "alta1-channel-stable-1"},
                 {"application_name": "base.bar", "intent": "direct",
                  "cluster_name": "alta1-channel-stable-1"},
                 {"application_name": "base.baz", "intent": "direct",
                  "cluster_name": null},
               ]
             }
           }

        :param application_name: The name of source application.
        :param cluster_name: The name of source cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The result is in the response.
        """
        check_application(application_name)
        check_cluster_name(cluster_name, application_name)
        facade = RouteManagement(huskar_client, application_name, cluster_name)
        route = sorted({
            'application_name': route[0], 'intent': route[1],
            'cluster_name': route[2],
        } for route in facade.list_route())
        return api_response({'route': route})

    @login_required
    def put(self, application_name, cluster_name, destination):
        """Changes the outgoing route of specific cluster.

        :param application_name: The name of source application.
        :param cluster_name: The name of source cluster.
        :param destination: The name of destination application.
        :form intent: The intent of route. (``direct``)
        :form cluster_name: The name of destination cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: Operation success.
        """
        # forbidden request when cluster_name in FORCE_ROUTING_CLUSTERS
        if cluster_name in settings.FORCE_ROUTING_CLUSTERS:
            abort(403, 'Can not modify {}\'s value'.format(cluster_name))
        self._check_auth(application_name, destination)
        check_cluster_name(cluster_name, application_name)
        self._put(application_name, cluster_name, destination)
        return api_response()

    @login_required
    def delete(self, application_name, cluster_name, destination):
        """Discards the outgoing route of specific cluster.

        :param application_name: The name of source application.
        :param cluster_name: The name of source cluster.
        :param destination: The name of destination application.
        :form intent: The intent of route. (``direct``)
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: Operation success.
        """
        self._check_auth(application_name, destination)
        check_cluster_name(cluster_name, application_name)
        self._delete(application_name, cluster_name, destination)
        return api_response()

    def _get_intent(self):
        intent = request.form.get('intent', ROUTE_DEFAULT_INTENT)
        if intent not in settings.ROUTE_INTENT_LIST:
            intent_list = u', '.join(settings.ROUTE_INTENT_LIST)
            abort(400, u'intent must be one of %s' % intent_list)
        return intent

    def _check_auth(self, application_name, dest_application_name):
        try:
            check_application_auth(dest_application_name, Authority.WRITE)
        except NoAuthError:
            check_application_auth(application_name, Authority.WRITE)

    @retry(OutOfSyncError, interval=1, max_retry=3)
    def _put(self, application_name, cluster_name, dest_application_name):
        dest_cluster_name = request.form['cluster_name'].strip()
        check_cluster_name(dest_cluster_name, dest_application_name)
        intent = self._get_intent()
        facade = RouteManagement(huskar_client, application_name, cluster_name)
        try:
            facade.set_route(dest_application_name, dest_cluster_name, intent)
        except EmptyClusterError as e:
            abort(400, unicode(e))
        audit_log.emit(
            audit_log.types.UPDATE_ROUTE, application_name=application_name,
            cluster_name=cluster_name, intent=intent,
            dest_application_name=dest_application_name,
            dest_cluster_name=dest_cluster_name)

    @retry(OutOfSyncError, interval=1, max_retry=3)
    def _delete(self, application_name, cluster_name, dest_application_name):
        intent = self._get_intent()
        facade = RouteManagement(huskar_client, application_name, cluster_name)
        dest_cluster_name = facade.discard_route(dest_application_name, intent)
        audit_log.emit(
            audit_log.types.DELETE_ROUTE, application_name=application_name,
            cluster_name=cluster_name, intent=intent,
            dest_application_name=dest_application_name,
            dest_cluster_name=dest_cluster_name)


class ServiceDefaultRouteView(MethodView):
    @login_required
    def get(self, application_name):
        """Gets the default route policy of specific application.

        Example of response::

           {
             "status": "SUCCESS",
             "message": "",
             "data": {
               "default_route": {
                 "overall": {
                   "direct": "channel-stable-2"
                 },
                 "altb1": {
                   "direct": "channel-stable-1"
                 }
               },
               "global_default_route": {
                 "direct": "channel-stable-2"
               }
             }
           }

        :param application_name: The name of specific application.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The result is in the response.
        """
        check_application(application_name)
        facade = RouteManagement(huskar_client, application_name, None)
        default_route = facade.get_default_route()
        return api_response({
            'default_route': default_route,
            'global_default_route': settings.ROUTE_DEFAULT_POLICY})

    @login_required
    @retry(OutOfSyncError, interval=1, max_retry=3)
    def put(self, application_name):
        """Creates or updates a default route policy of specific application.

        :param application_name: The name of specific application.
        :form ezone: Optional. The ezone of default route. Default: ``overall``
        :form intent: Optional. The intent of default route. Default:
                      ``direct``
        :form cluster_name: The name of destination cluster. The cluster name
                            must not be ezone prefixed.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: Operation success.
        """
        check_application_auth(application_name, Authority.WRITE)
        ezone = request.form.get('ezone') or OVERALL
        intent = request.form.get('intent') or ROUTE_DEFAULT_INTENT
        cluster_name = request.form['cluster_name']
        check_cluster_name(cluster_name, application_name)
        facade = RouteManagement(huskar_client, application_name, None)
        try:
            default_route = facade.set_default_route(
                ezone, intent, cluster_name)
        except ValueError as e:
            # TODO: Use a better validator instead
            return api_response(
                status='InvalidArgument', message=first(e.args, '')), 400
        audit_log.emit(
            audit_log.types.UPDATE_DEFAULT_ROUTE,
            application_name=application_name, ezone=ezone, intent=intent,
            cluster_name=cluster_name)
        return api_response({
            'default_route': default_route,
            'global_default_route': settings.ROUTE_DEFAULT_POLICY})

    @login_required
    def delete(self, application_name):
        """Discards a default route policy of specific application.

        :param application_name: The name of specific application.
        :form ezone: Optional. The ezone of default route. Default: ``overall``
        :form intent: Optional. The intent of default route. Default:
                      ``direct``
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: Operation success.
        """
        check_application_auth(application_name, Authority.WRITE)
        ezone = request.form.get('ezone') or OVERALL
        intent = request.form.get('intent') or ROUTE_DEFAULT_INTENT
        facade = RouteManagement(huskar_client, application_name, None)
        try:
            default_route = facade.discard_default_route(ezone, intent)
        except ValueError as e:
            # TODO: Use a better validator instead
            return api_response(
                status='InvalidArgument', message=first(e.args, '')), 400
        audit_log.emit(
            audit_log.types.DELETE_DEFAULT_ROUTE,
            application_name=application_name, ezone=ezone, intent=intent)
        return api_response({
            'default_route': default_route,
            'global_default_route': settings.ROUTE_DEFAULT_POLICY})
