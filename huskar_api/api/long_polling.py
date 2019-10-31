from __future__ import absolute_import

import logging

from flask import request, json, abort, g, stream_with_context, Response
from flask.views import MethodView
from huskar_sdk_v2.consts import CONFIG_SUBDOMAIN, SERVICE_SUBDOMAIN

from huskar_api import settings
from huskar_api.extras.raven import capture_exception
from huskar_api.extras.concurrent_limiter import release_after_iterator_end
from huskar_api.models import huskar_client
from huskar_api.models.auth import Authority
from huskar_api.models.route import RouteManagement
from huskar_api.models.route.hijack import RouteHijack
from huskar_api.models.tree import TreeHub, TreeHolderCleaner
from huskar_api.service.admin.application_auth import (
    check_application, check_application_auth)
from huskar_api.service.utils import check_cluster_name
from huskar_api.switch import (
    switch, SWITCH_ENABLE_DECLARE_UPSTREAM)
from .utils import login_required, get_life_span
from .schema import event_subscribe_schema


tree_hub = TreeHub(huskar_client, settings.TREE_HOLDER_STARTUP_MAX_CONCURRENCY)
tree_holder_cleaner = TreeHolderCleaner(tree_hub)
tree_holder_cleaner.spawn_cleaning_thread()


class LongPollingView(MethodView):
    @login_required
    def post(self):
        """Subscribes changes of service, switch or config via HTTP long
        polling connection.

        This API accepts requests in following schema::

            {
              "service": {        # "service" / "switch" / "config"
                "base.foo": [     # the application name
                  "cluster-bar",  # the cluster name
                ]
              }
            }

        Put it in your request body and set ``Content-Type: application/json``
        in the request header, you will receive event stream in the response
        body as ``\\n`` splited JSON fragments.

        The events in all clusters will be subscribed, if the cluster list is
        empty.

        The placeholder of cluster name accepts "intent" also, if you enables
        the route mode via the HTTP header ``X-SOA-Mode``. See also
        :ref:`Traffic Control <traffic_control>`.

        The types of event messages in response are described in
        :ref:`long-polling-message-types`.

        :query trigger: Optional. Passing ``1`` will let us dump all existed
                        data in the connection initial. Default is ``1``.
        :query life_span: Optional. How many seconds before the stream end.
                          Default is ``3600``.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header Content-Type: :mimetype:`application/json`
        :<header X-SOA-Mode: The SOA mode of service consumer
                             (See :ref:`SOA route <traffic_control_route>`)
        :<header X-Cluster-Name: The cluster name of service consumer
                                 (See :ref:`SOA route <traffic_control_route>`)
        :status 400: The request schema is invalid.
        :status 200: Subscription is okay. You could read the event stream from
                     response body now.
        """
        trigger = request.args.get('trigger', type=int, default=1)
        life_span = request.args.get('life_span', type=int, default=0)
        request_data = self.get_request_data()

        tree_watcher = tree_hub.make_watcher(
            with_initial=bool(trigger),
            life_span=get_life_span(max(life_span, 0)),
            from_application_name=g.application_name,
            from_cluster_name=g.cluster_name,
            metrics_tag_from=g.auth.username,
        )

        # TODO Add timing metrics here
        request_data, tree_watcher_decorator = self.learn_and_hijack_route(
            request_data, tree_watcher)
        response_iterator = self.perform(
            request_data, tree_watcher, tree_watcher_decorator)

        # Wait for being started
        next(response_iterator)

        return Response(stream_with_context(response_iterator))

    def perform(self, request_data, tree_watcher, tree_watcher_decorator):
        """Performs long polling session as an iterator.

        The iterator returned by this method will always generate a ``None``
        in the first time. It indicates that the watcher is started and we
        could put the iterator into a response stream now.

        .. note:: You should never touch the database (SQLAlchemy session)
                  here or the connection may be leaked. The request context
                  has been finalized here.

        .. note:: The returned iterator is not thread safe. Do not try to use
                  it in different greenlets.
        """
        self.declare_upstream_from_request(request_data)

        # TODO Add timing metrics here
        for type_name, application_names in request_data.items():
            for application_name, cluster_names in application_names.items():
                tree_watcher.watch(application_name, type_name)
                for cluster_name in cluster_names:
                    tree_watcher.limit_cluster_name(
                        application_name, type_name, cluster_name)

        yield

        tree_watcher = tree_watcher_decorator(tree_watcher)
        for message, body in tree_watcher:
            line = json.dumps({'message': message, 'body': body}) + '\n'
            yield line

    def get_request_data(self):
        request_data = request.get_json()
        if not isinstance(request_data, dict):
            abort(400, 'JSON payload must be present and match its schema')

        request_data = event_subscribe_schema.load(request_data).data
        for type_name, application_names in request_data.iteritems():
            for application_name, cluster_names in application_names.items():
                for cluster_name in cluster_names:
                    check_cluster_name(cluster_name, application_name)
                if type_name == CONFIG_SUBDOMAIN:
                    check_application_auth(application_name, Authority.READ)
                else:
                    check_application(application_name)

                tree_holder_cleaner.track(application_name, type_name)

        return request_data

    def declare_upstream_from_request(self, request_data):
        if not g.auth.is_application or not g.cluster_name:
            return
        if not switch.is_switched_on(SWITCH_ENABLE_DECLARE_UPSTREAM):
            return
        route_management = RouteManagement(
            huskar_client, g.auth.username, g.cluster_name)
        application_names = frozenset(request_data.get(SERVICE_SUBDOMAIN, []))
        try:
            route_management.declare_upstream(application_names)
        except Exception:
            capture_exception(level=logging.WARNING)

    def learn_and_hijack_route(self, request_data, tree_watcher):
        request_domain = request.host.split(':')[0]
        route_hijack = RouteHijack(
            huskar_client, g.auth.username, g.cluster_name,
            request.remote_addr, g.route_mode, request_domain)

        route_hijack.prepare(tree_watcher, request_data)
        request_data = route_hijack.hijack_request(request_data)

        def tree_watcher_decorator(tree_watcher):
            tree_watcher = route_hijack.hijack_response(tree_watcher)
            tree_watcher = release_after_iterator_end(
                g.get('concurrent_limiter_data'), tree_watcher)
            return tree_watcher

        return request_data, tree_watcher_decorator
