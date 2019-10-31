from __future__ import absolute_import

import logging

from flask import json, request, abort, g
from flask.views import MethodView
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN

from huskar_api.models import huskar_client
from huskar_api.models.auth import Authority

from huskar_api.models.exceptions import OutOfSyncError, ContainerUnboundError
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.container import ContainerManagement, is_container_id
from huskar_api.extras.raven import capture_exception
from huskar_api.service import service as service_facade
from huskar_api.service.admin.application_auth import (
    check_application_auth, check_application)
from huskar_api.service.exc import ServiceValueError
from huskar_api.service.utils import check_cluster_name
from .instance import InstanceFacade
from .schema import instance_schema, service_value_schema, validate_fields
from .utils import login_required, api_response, audit_log


logger = logging.getLogger(__name__)


class ServiceInstanceView(MethodView):
    @login_required
    def get(self, application_name, cluster_name):
        """Discovers service instances in specified application and cluster.

        The ``read`` authority is **NOT** required because service registry
        is in the public area of Huskar.

        The response looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": [
                {
                  "application": "base.foo",
                  "cluster": "stable",
                  "key": "DB_URL",
                  "value": "{...}"
                }
              ]
            }

        If the ``key`` is specified, the ``data`` field in response will be
        an object directly without :js:class:`Array` around.

        :query key: Optional. The same as :ref:`config`.
        :query resolve: Optional. Resolve linked cluster or not. ``0`` or ``1``
                        ``0``: Don't resolve, ``1``: Resolve (default).
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application, cluster or key is not found.
        :status 200: The result is in the response.
        """
        check_application(application_name)
        check_cluster_name(cluster_name, application_name)
        facade = InstanceFacade(SERVICE_SUBDOMAIN, application_name,
                                include_comment=False)
        key = request.args.get('key')
        resolve = request.args.get('resolve', '1') != '0'
        validate_fields(instance_schema, {
            'key': key,
            'cluster': cluster_name,
            'application': application_name,
        }, optional_fields=['key'])

        if key:
            instance = facade.get_instance(cluster_name, key, resolve=resolve)
            if instance is None:
                abort(404, '%s %s/%s/%s does not exist' % (
                    SERVICE_SUBDOMAIN, application_name, cluster_name, key,
                ))
            return api_response(instance)
        else:
            instance_list = facade.get_instance_list_by_cluster(
                cluster_name, resolve=resolve)
            return api_response(instance_list)

    @login_required
    def post(self, application_name, cluster_name):
        """Registers a service instance.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster. Unlike :ref:`switch`,
                             using ``overall`` here is pointless for
                             service discovery.
        :form key: The name of instance. (e.g. ``10.0.0.1_5000``)
        :form value: The value of service which should be a JSON string and
                     include ``ip``, ``port``, ``state`` and also. Example::

                        {
                          "ip": "10.0.0.1",
                          "port": {
                            "main": 5000,  // "main" should be provided always
                          },
                          "state": "up",   // "up" or "down"
                          ...
                        }
        :form runtime: The runtime value of service which should be a JSON
                       string too. When this parameter is provided, the
                       ``value`` could be ignored. Only ``state`` could be
                       included in runtime value, for updating the state of a
                       service instance in a concurrence safe way. Example::

                          {"state": "down"}
        :form version: The version of instance, its optional.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application is not found.
        :status 400: The request body is invalid.
        :status 200: The service instance is registered successfully.
        :status 409: The version is outdated, resource is modified by
                     another request.
        """
        check_application_auth(application_name, Authority.WRITE)
        check_cluster_name(cluster_name, application_name)

        key = request.values['key'].strip()
        value = self._get_value()
        runtime = self._get_runtime()
        version = request.form.get('version', type=int)

        if not value and not runtime:
            abort(400, 'either "value" or "runtime" should be provided.')
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name,
            'key': key
        })
        service_instance_facade = ServiceInstanceFacade(
            application_name, cluster_name, key
        )

        # Writes container registry firstly
        if is_container_id(key):
            try:
                service_instance_facade.register_container()
            except ContainerUnboundError:
                abort(409, "this container has been unbound recently")
        try:
            new_data = service_instance_facade.create_or_update_instance(
                service_facade, value, version, runtime
            )
        except ServiceValueError as e:
            abort(400, e.args[0])
        except OutOfSyncError:
            abort(409, 'resource is modified by another request')
        else:
            return api_response({
                'value': new_data.data,
                'meta': InstanceFacade.make_meta_info(new_data),
            })

    @login_required
    def put(self, application_name, cluster_name):  # pragma: no cover
        return self.post(application_name, cluster_name)

    @login_required
    def delete(self, application_name, cluster_name):
        """Deregisters a service instance.

        The ``write`` authority is required. See :ref:`application_auth` also.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster.
        :form key: The name of instance.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application is not found.
        :status 400: The request body is invalid.
        :status 200: The service instance is deregistered successfully.
        """
        check_application_auth(application_name, Authority.WRITE)
        check_cluster_name(cluster_name, application_name)

        key = request.values['key'].strip()
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name,
            'key': key,
        })

        im = InstanceManagement(
            huskar_client, application_name, SERVICE_SUBDOMAIN)
        instance, _ = im.get_instance(cluster_name, key, resolve=False)
        if instance.stat is None:
            abort(404, '%s %s/%s/%s does not exist' % (
                SERVICE_SUBDOMAIN, application_name, cluster_name, key,
            ))

        old_data = instance.data
        with audit_log(audit_log.types.DELETE_SERVICE,
                       application_name=application_name,
                       cluster_name=cluster_name, key=key, old_data=old_data):
            service_facade.delete(
                application_name, cluster_name, key, strict=True)

        # Writes container registry finally
        if is_container_id(key):
            cm = ContainerManagement(huskar_client, key)
            cm.deregister_from(application_name, cluster_name)

        return api_response()

    def _get_value(self):
        # TODO: remove this function if no more deprecated fields
        # occurred.
        def log_for_deprecated_fields(value):
            schema_fields = {'ip', 'port', 'state', 'meta'}
            fields = set(value)
            if not schema_fields.issuperset(set(value)):
                logger.info('Deprecated fields of service meta: %s', fields)

        if 'value' not in request.values:
            return
        value = request.values.get('value', type=json.loads)
        if value is None:
            abort(400, 'request data is not json format.')
        log_for_deprecated_fields(value)
        return service_value_schema.load(value).data

    def _get_runtime(self):
        if 'runtime' not in request.values:
            return
        runtime = request.values.get('runtime', type=json.loads)
        if runtime is None:
            abort(400, 'request data is not json format.')
        if set(runtime) != {'state'}:
            abort(400, 'runtime must contain state only.')
        if runtime.get('state') not in ('up', 'down'):
            abort(400, 'runtime state must be "up" or "down".')
        return runtime


class ServiceInstanceWeightView(MethodView):

    @login_required
    def get(self, application_name, cluster_name, key):
        """Gets the weight of specified service instance.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster.
        :param key: The key of service instance.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The instance is not found.
        :status 200: The response looks like
                     ``{"status":"SUCCESS",
                        "data": {"weight": 10}}``
        """
        check_cluster_name(cluster_name, application_name)
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name,
            'key': key,
        })

        im = InstanceManagement(
            huskar_client, application_name, SERVICE_SUBDOMAIN)
        instance, _ = im.get_instance(cluster_name, key, resolve=False)

        if instance.stat is None:
            abort(404, '%s %s/%s/%s does not exist' % (
                SERVICE_SUBDOMAIN, application_name, cluster_name, key,
            ))

        try:
            data = json.loads(instance.data)
            weight = int(data['meta']['weight'])
        except (ValueError, TypeError, KeyError):
            weight = 0

        return api_response({'weight': weight})

    @login_required
    def post(self, application_name, cluster_name, key):
        """Updates the weight of specified service instance.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster.
        :param key: The key of service instance.
        :form weight: The target weight of instance. It should be a positive
                      integer.
        :form ephemeral: Whether the modification be ephemeral or persistent.
                         Must be ephemeral (``1``) for now.
        :form ttl: When ephemeral is ``1``, will use this value as ttl.
                   default: 5 * 60 s.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The request body is invalid.
        :status 404: The instance is not found.
        :status 409: There is another request has modified the instance.
        :status 200: The weight is updated successfully.
        """
        check_application_auth(application_name, Authority.WRITE)
        check_cluster_name(cluster_name, application_name)
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name,
            'key': key,
        })

        weight = request.form.get('weight', type=int)
        if not weight or weight < 0:
            abort(400, 'weight must be a positive integer')
        weight = unicode(weight)
        ephemeral = request.form.get('ephemeral', type=int)
        if ephemeral != 1:
            abort(400, 'ephemeral must be "1" for now')

        im = InstanceManagement(
            huskar_client, application_name, SERVICE_SUBDOMAIN)
        instance, _ = im.get_instance(cluster_name, key, resolve=False)

        if instance.stat is None:
            abort(404, '%s %s/%s/%s does not exist' % (
                SERVICE_SUBDOMAIN, application_name, cluster_name, key,
            ))

        try:
            old_data = instance.data
            new_data = json.loads(instance.data)
            meta = new_data.setdefault('meta', {})
            meta['weight'] = weight
            new_data = json.dumps(new_data)
            instance.data = new_data
        except (ValueError, TypeError):
            capture_exception()  # unexpected exception should be captured
            abort(500)

        try:
            instance.save()
        except OutOfSyncError:
            abort(409, '%s %s/%s/%s has been modified by another request' % (
                SERVICE_SUBDOMAIN, application_name, cluster_name, key,
            ))

        audit_log.emit(
            audit_log.types.UPDATE_SERVICE,
            application_name=application_name,
            cluster_name=cluster_name,
            key=key,
            old_data=old_data,
            new_data=new_data)

        return api_response()


class ServiceRegistryView(MethodView):
    def __init__(self):
        self._wrapped = ServiceInstanceView()

    def check_context(self):
        if not g.auth.is_application:
            abort(403, 'Authorization with an application token is required')
        if not g.cluster_name:
            abort(403, 'X-Cluster-Name is required')

    @login_required
    def post(self):
        """Registers a service instance of current application.

        This is a drop-in replacement for the original service instance API. It
        requires minimal parameters and retrieves information from the request
        context itself, such as token and meta headers.

        :form key: The name of instance. (e.g. ``10.0.0.1_5000``)
        :form value: The value of service which should be a JSON string and
                     include ``ip``, ``port``, ``state`` and also. Example::

                        {
                          "ip": "10.0.0.1",
                          "port": {
                            "main": 5000,  // "main" should be provided always
                          },
                          "state": "up",   // "up" or "down"
                          ...
                        }
        :form runtime: The runtime value of service which should be a JSON
                       string too. When this parameter is provided, the
                       ``value`` could be ignored. Only ``state`` could be
                       included in runtime value, for updating the state of a
                       service instance in a concurrence safe way. Example::

                          {"state": "down"}
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header X-Cluster-Name: The cluster name of current service instance.
        :status 400: The request body is invalid.
        :status 403: The token or meta headers is not suitable.
        :status 404: The application is not found.
        :status 200: The service instance is registered successfully.
        """
        self.check_context()
        return self._wrapped.post(g.auth.username, g.cluster_name)

    @login_required
    def delete(self):
        """Deregisters a service instance of current application.

        This is a drop-in replacement for the original service instance API. It
        requires minimal parameters and retrieves information from the request
        context itself, such as token and meta headers.

        :form key: The name of instance.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header X-Cluster-Name: The cluster name of current service instance.
        :status 400: The request body is invalid.
        :status 403: The token or meta headers is not suitable.
        :status 404: The application is not found.
        :status 200: The service instance is deregistered successfully.
        """
        self.check_context()
        return self._wrapped.delete(g.auth.username, g.cluster_name)


class ServiceInstanceFacade(object):

    def __init__(self, application_name, cluster_name, key):
        self.application_name = application_name
        self.cluster_name = cluster_name
        self.key = key

    def register_container(self):
        cm = ContainerManagement(huskar_client, self.key)
        cm.raise_for_unbound(
            self.application_name,
            self.cluster_name,
            self.key
        )
        cm.register_to(self.application_name, self.cluster_name)

    def create_or_update_instance(self, facade, value, version, runtime=None):
        old_data = facade.get_value(
            self.application_name, self.cluster_name, self.key)
        new_data = facade.save(
            application=self.application_name, cluster=self.cluster_name,
            key=self.key, value=value, version=version, runtime=runtime)
        audit_log.emit(
            audit_log.types.UPDATE_SERVICE,
            application_name=self.application_name,
            cluster_name=self.cluster_name, key=self.key,
            old_data=old_data, new_data=json.dumps(new_data.data))

        return new_data
