from __future__ import absolute_import

import logging
import itertools
from operator import attrgetter

from flask import request, abort, g
from flask.views import MethodView

from huskar_api.models.webhook import Webhook
from huskar_api.models.audit import action_types
from huskar_api.models.auth import Application, Authority
from huskar_api.service.admin.application_auth import (
    check_application_auth, check_application)
from .utils import login_required, api_response, minimal_mode_incompatible
from .schema import webhook_schema, validate_fields


logger = logging.getLogger(__name__)


class WebhookView(MethodView):
    @login_required
    @minimal_mode_incompatible
    def get(self):
        """List all webhooks registered in Huskar.

        The response looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": {
                "webhook_list": [
                    {
                        "webhook_id": 1,
                        "webhook_url": "http://www.example.com",
                        "webhook_type": 0
                    }
                ]
              }
            }

        :status 200: The request is successful.
        :status 404: The application not found.
        """
        webhooks = Webhook.get_all()
        webhook_list = [{
            'webhook_id': webhook.id,
            'webhook_url': webhook.url,
            'webhook_type': webhook.hook_type
        } for webhook in webhooks]
        return api_response(data={'webhook_list': webhook_list})

    @login_required
    @minimal_mode_incompatible
    def post(self):
        """Create a new webhook.

        The request accepting a JSON body, the schema likes::

            {
                "webhook_url": "http://www.example.com",
                "event_list": [
                    "CREATE_CONFIG_CLUSTER",
                    "DELETE_CONFIG_CLUSTER"
                ]
            }

        The content of ``event_list`` should be a list of action that
        already defined in Huskar.

        The ``application_name`` is only required when the ``webhook_type``
        is 0, it means the webhook want to subscribe some events of specified
        application. If the ``webhook_type`` value specified with 1,
        a universal webhook will be registered which will receive all
        the events of Huskar site, so the ``event_list`` will be ignored
        because that is unnecessary.

        :param webhook_type: default 0, set ``site`` level with 1.
        :param application_name: The name of application, optional.
        :form webhook_url: the webhook url.
        :form event_list: event list want subscribed
        :status 404: The application not found.
        :status 200: successful request.
        """
        webhook_type = request.args.get('webhook_type', default=0, type=int)
        self._check_authority(webhook_type)
        data = request.get_json() or {}
        validate_fields(webhook_schema, data, partial=False)

        if webhook_type == Webhook.TYPE_UNIVERSAL:
            webhook = Webhook.create(data['webhook_url'], webhook_type)
            return api_response()
        application_name = request.args['application_name']
        application = Application.get_by_name(application_name)
        webhook = Webhook.create(data['webhook_url'], webhook_type)
        for action_name in data.get('event_list', []):
            action_type = getattr(action_types, action_name)
            webhook.subscribe(application.id, action_type)
        return api_response()

    def _check_authority(self, webhook_type):
        if webhook_type == Webhook.TYPE_UNIVERSAL:
            g.auth.require_admin('only admin can add universal webhook')
        else:
            application_name = request.args['application_name']
            check_application_auth(application_name, Authority.WRITE)


class WebhookInstanceView(MethodView):
    @login_required
    @minimal_mode_incompatible
    def get(self, webhook_id):
        """Get the webhook subscriptions list of specified application and
        The ``read`` authority is required.

        The response looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": {
                "webhook_id": 1,
                "webhook_url": "http://www.example.com",
                "webhook_type": 0,
                "event_list": [
                    "CREATE_CONFIG_CLUSTER",
                    "DELETE_CONFIG_CLUSTER",
                    ...
                ]
              }
            }

        The content of ``event_list`` is a list of action that
        already defined in Huskar.

        :param application_name: The name of application.
        :status 200: The request is successful.
        :status 404: The application not found.
        """
        webhook = self._get_webhook_or_404(webhook_id)
        if not webhook.is_normal:
            return api_response(data={
                'webhook_id': webhook.id,
                'webhook_url': webhook.url,
                'webhook_type': webhook.hook_type,
                'event_list': []
            })

        application_name = request.args['application_name']
        check_application_auth(application_name, Authority.READ)
        application = Application.get_by_name(application_name)
        subscriptions = webhook.get_multi_subscriptions(application.id)
        data = {
            'webhook_id': webhook.id,
            'webhook_url': webhook.url,
            'webhook_type': webhook.hook_type,
            'event_list': [action_types[x.action_type] for x in subscriptions]
        }
        return api_response(data=data)

    @login_required
    @minimal_mode_incompatible
    def put(self, webhook_id):
        """To update subscription settings of an application

        Request body schema same to ``POST`` method.

        :param application_name: The name of application.
        :param webhook_id: The id of webhook.
        :param webhoo_type: default 0, set universal with 1
        :status 200: thr request is successful.
        :status 404: The application or webhoook not found.
        """
        webhook = self._get_webhook_or_404(webhook_id)
        self._check_authority(webhook)

        data = request.get_json() or {}
        validate_fields(webhook_schema, data, partial=False)
        if not webhook.is_normal:
            webhook.update_url(data['webhook_url'])
            return api_response()

        application_name = request.args['application_name']
        application = Application.get_by_name(application_name)
        webhook.batch_unsubscribe(application.id)
        webhook.update_url(data['webhook_url'])
        for action_name in data.get('event_list', []):
            action_type = getattr(action_types, action_name)
            webhook.subscribe(application.id, action_type)
        return api_response()

    @login_required
    @minimal_mode_incompatible
    def delete(self, webhook_id):
        """Unsubscribe all subscriptions of the webhook with
        specified ``webhook_id``, and delete the webhook.

        The ``application_name`` is required when the webhook subscribe
        application level events.

        :param application_name: The name of application, optional.
        :param webhook_id: The id of webhook.
        :status 200: thr request is successful
        :status 404: The application or webhook not found.
        """
        webhook = self._get_webhook_or_404(webhook_id)
        self._check_authority(webhook.hook_type)
        if webhook.is_normal:
            webhook = self._get_webhook_or_404(webhook_id)
            webhook.batch_unsubscribe()
        webhook.delete()
        return api_response()

    def _get_webhook_or_404(self, webhook_id):
        instance = Webhook.get(webhook_id)
        if not instance:
            abort(404, 'Webhook not registered.')
        return instance

    def _check_authority(self, webhook_type):
        # TODO: fix that duplicated code
        if webhook_type == Webhook.TYPE_UNIVERSAL:
            g.auth.require_admin('only admin can update universal webhook')
        else:
            application_name = request.args['application_name']
            check_application_auth(application_name, Authority.WRITE)


class ApplicationWebhookView(MethodView):
    @login_required
    @minimal_mode_incompatible
    def get(self, application_name):
        """List the subscriptions of an application specified with
        the ``application_name``.

        The response looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": {
                "webhook_list": [
                  {
                    "webhook_id": 1,
                    "webhook_url": "http://www.example.com",
                    "webhook_type": 0,
                    "event_list": [
                        "CREATE_CONFIG_CLUSTER",
                        "DELETE_CONFIG_CLUSTER",
                        ...
                    ]
                  },
                  ...
                ]
              }
            }

        :param application_name: The name of application.
        :status 200: The request is successful.
        """
        application = check_application(application_name)
        subscriptions = Webhook.search_subscriptions(
            application_id=application.id)
        groups = itertools.groupby(subscriptions, key=attrgetter('webhook_id'))
        webhook_list = []
        for webhook_id, group in groups:
            webhook = Webhook.get(webhook_id)
            webhook_list.append({
                'webhook_id': webhook.id,
                'webhook_url': webhook.url,
                'webhook_type': webhook.hook_type,
                'event_list': [action_types[x.action_type] for x in group]
            })
        return api_response(data={'webhook_list': webhook_list})
