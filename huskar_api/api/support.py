from __future__ import absolute_import

import contextlib

from flask import abort, request, json, g
from flask.views import MethodView
from huskar_sdk_v2.consts import CONFIG_SUBDOMAIN, OVERALL
from ipaddress import ip_address

from huskar_api.models import huskar_client
from huskar_api.models.auth import Authority
from huskar_api.models.route.hijack import RouteHijack
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.container import ContainerManagement
from huskar_api.models.exceptions import OutOfSyncError, NotEmptyError
from huskar_api.service import service as service_facade
from huskar_api.service.admin.application_auth import (
    check_application_auth, check_application)
from huskar_api.service.admin.exc import NoAuthError
from huskar_api.settings import APP_NAME
from .utils import login_required, api_response, audit_log


class CurrentUserView(MethodView):
    def get(self):
        """Gets information of current user.

        The ``is_minimal_mode`` indicates whether it is in a databsae outage.

        An example of response::

            {
              "status": "SUCCESS",
              "data": {
                "is_anonymous": false,
                "is_application": true,
                "is_minimal_mode": false,
                "is_admin": false,
                "username": "foo.test"
              }
            }

        :<header Authorization: Optional Huskar Token (See :ref:`token`)
        :status 200: This API always returns 200.
        """
        if g.auth:
            is_anonymous = False
            is_application = g.auth.is_application
            is_minimal_mode = g.auth.is_minimal_mode
            is_admin = g.auth.is_admin
            username = g.auth.username
        else:
            is_anonymous = True
            is_application = False
            is_minimal_mode = False
            is_admin = False
            username = ''
        return api_response({
            'is_anonymous': is_anonymous,
            'is_application': is_application,
            'is_minimal_mode': is_minimal_mode,
            'is_admin': is_admin,
            'username': username,
        })


class ContainerRegistryView(MethodView):
    @login_required
    def get(self, container_id):
        """Gets all instances bound to this container.

        An example of response::

            {
              "status": "SUCCESS",
              "data": {
                "registry": [
                  {
                    "application_name": "base.foo",
                    "cluster_name": "test_cluster"
                  }
                ]
              }
            }

        :param container_id: The container id (a.k.a CID, Task ID).
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The request is successful.
        """
        cm = ContainerManagement(huskar_client, container_id)
        registry = [
            {'application_name': a, 'cluster_name': c} for a, c in cm.lookup()]
        barrier = cm.has_barrier()
        return api_response({'registry': registry, 'barrier': barrier})

    @login_required
    def delete(self, container_id):
        """Reregisters all instances bound to this container.

        The site admin authority is required. See :ref:`site_admin` also.

        :param container_id: The container id (a.k.a CID, Task ID).
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 409: The container is still using by another one, and who are
                     registering new service instance on it. It is recommended
                     to try again since the container is truly dead.
        :status 200: The request is successful.
        """
        g.auth.require_admin()

        cm = ContainerManagement(huskar_client, container_id)
        cm.set_barrier()

        for application_name, cluster_name in cm.lookup():
            # Deregister current instance
            old_data = service_facade.get_value(
                application_name, cluster_name, container_id)
            service_facade.delete(
                application_name, cluster_name, container_id, strict=False)
            audit_log.emit(
                audit_log.types.DELETE_SERVICE,
                application_name=application_name, cluster_name=cluster_name,
                key=container_id, old_data=old_data)
            # Release container record
            cm.deregister_from(application_name, cluster_name)

        try:
            cm.destroy()
        except NotEmptyError:
            message = 'Container {0} is still registering new instance'.format(
                container_id)
            abort(409, message)

        return api_response()


class BlacklistView(MethodView):
    _KEY = 'AUTH_IP_BLACKLIST'

    @login_required
    def get(self):
        """Gets the blacklist of IP address.

        Those IP addresses will be denied to access any part of Huskar API.

        The site admin authority is required. See :ref:`application_auth` also.

        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: ``{"data": {"blacklist": ["127.0.0.1"]}``
        """
        g.auth.require_admin()
        instance, _ = self._make_im().get_instance(OVERALL, self._KEY)
        blacklist = json.loads(instance.data) if instance.data else []
        return api_response({'blacklist': sorted(blacklist)})

    @login_required
    def post(self):
        """Adds an IP address to blacklist.

        The site admin authority is required. See :ref:`application_auth` also.

        :<header Authorization: Huskar Token (See :ref:`token`)
        :form remote_addr: The adding IP address.
        :status 400: The IP address is invalid.
        :status 409: The blacklist is modifying by another request.
        :status 200: The operation is success.
        """
        g.auth.require_admin()
        remote_addr = request.form.get('remote_addr', type=ip_address)
        if not remote_addr:
            abort(400, 'remote_addr is invalid')
        with self._update_blacklist() as blacklist:
            blacklist.add(unicode(remote_addr))
        return api_response()

    @login_required
    def delete(self):
        """Deletes an IP address from blacklist.

        The site admin authority is required. See :ref:`application_auth` also.

        :<header Authorization: Huskar Token (See :ref:`token`)
        :form remote_addr: The deleting IP address.
        :status 400: The IP address is invalid.
        :status 409: The blacklist is modifying by another request.
        :status 200: The operation is success.
        """
        g.auth.require_admin()
        remote_addr = request.form.get('remote_addr', type=ip_address)
        if not remote_addr:
            abort(400, 'remote_addr is invalid')
        with self._update_blacklist() as blacklist:
            blacklist.discard(unicode(remote_addr))
        return api_response()

    @contextlib.contextmanager
    def _update_blacklist(self):
        instance, _ = self._make_im().get_instance(OVERALL, self._KEY)
        blacklist = set(json.loads(instance.data) if instance.data else [])
        yield blacklist
        instance.data = json.dumps(sorted(blacklist), indent=2)
        try:
            instance.save()
        except OutOfSyncError:
            abort(409, 'resource is modified by another request')

    def _make_im(self):
        return InstanceManagement(huskar_client, APP_NAME, CONFIG_SUBDOMAIN)


class RouteProgramView(MethodView):
    _KEY = 'ROUTE_HIJACK_LIST'

    @login_required
    def get(self):
        """Gets the stage of route program.

        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: ``{"data": {"route_stage": {"base.foo": "D"}}``
        """
        cluster_name = request.args.get('cluster', OVERALL)
        instance, _ = self._make_im().get_instance(cluster_name, self._KEY)
        route_stage = json.loads(instance.data) if instance.data else {}
        return api_response({'route_stage': route_stage})

    @login_required
    def post(self):
        """Changes the stage of route program.

        The site admin authority is required. See :ref:`application_auth` also.

        :<header Authorization: Huskar Token (See :ref:`token`)
        :form application: The name of application
        :form stage: **D**isabled / **C**hecking / **E**nabled /
                     **S**tandardalone.
        :status 400: The parameters are invalid.
        :status 409: The list is modifying by another request.
        :status 200: The operation is success.
        """
        name = request.form['application']
        stage = request.form.get('stage', type=RouteHijack.Mode)
        cluster_name = request.form.get('cluster', OVERALL)
        cluster_list = {OVERALL}.union(self._make_im().list_cluster_names())
        if stage is None:
            abort(400, 'stage is invalid')
        if cluster_name not in cluster_list:
            abort(400, 'cluster is invalid')
        try:
            g.auth.require_admin()
            check_application(name)
        except NoAuthError:
            check_application_auth(name, Authority.WRITE)
        with self._update_hijack_list(cluster_name) as hijack_list:
            old_stage = hijack_list.pop(name, RouteHijack.Mode.disabled.value)
            hijack_list[name] = stage.value
        if old_stage != stage.value:
            audit_log.emit(
                audit_log.types.PROGRAM_UPDATE_ROUTE_STAGE,
                application_name=name, old_stage=old_stage,
                new_stage=stage.value)
        return api_response({'route_stage': hijack_list})

    @contextlib.contextmanager
    def _update_hijack_list(self, cluster_name):
        instance, _ = self._make_im().get_instance(cluster_name, self._KEY)
        hijack_list = dict(json.loads(instance.data) if instance.data else {})
        hijack_list_shadow = dict(hijack_list)
        yield hijack_list
        if hijack_list == hijack_list_shadow:
            return
        instance.data = json.dumps(dict(hijack_list))
        try:
            instance.save()
        except OutOfSyncError:
            abort(409, 'resource is modified by another request')

    def _make_im(self):
        return InstanceManagement(huskar_client, APP_NAME, CONFIG_SUBDOMAIN)
