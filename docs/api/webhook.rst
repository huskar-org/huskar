.. _webhook:

Webhook
=======

Webhook allows you to subscribe to certain events on Huskar at ``application`` or ``site`` level.
When one of those events is triggered, Huskar will send a **HTTP POST** payload to the webhook's configured URL.


Webhook Type
------------

There are two kinds of webhook to subscribe different event level.

1. The ``application`` webhook can subscribe some action events of an specified application.
2. The ``site`` webhook will receive notifications of all action events in Huskar.

You can add an integer type argument ``webhook_type`` in HTTP **query string** to specified the type of webhook

============== ==================
Event Level    webhook_type
-------------- ------------------
application    ``0``
site           ``1``
============== ==================


Webhook HTTP Request
--------------------

Each event type has specific payload format with the relevant event information.

* ``application_names``: The list of involved applications
* ``username``: Operator's username
* ``user_type``: Type of user, ``0`` is normal user, ``1`` is application user
* ``severity``: Severity of action, ``0`` is normal action, ``1`` is dangerous action
* ``action_name``: The name of action
* ``action_data``: The data of action

Example delivery
^^^^^^^^^^^^^^^^

::

    POST /payload HTTP/1.1
    Content-Type: application/json

    {
      "application_names": ["test.webhook"],
      "username": "san.zhang",
      "user_type": 0,
      "severity": 1,
      "action_name": "UPDATE_SWITCH",
      "action_data": {
        "application_name": "test.webhook",
        "cluster_name": "overall",
        "key": "test"
      }
    }


There is not fixed format of ``action_data``, but we pretty sure it has enough information to describe the action.


.. _events:

Huskar Events
------------------

The available events are:

=============================== ========================================= =================
Event Name                      action_data example                       application level
------------------------------- ----------------------------------------- -----------------
UPDATE_SERVICE                  |update_instance_example|                 YES
DELETE_SERVICE                  |update_instance_example|                 YES
CREATE_SERVICE_CLUSTER          |create_cluster_example|                  YES
DELETE_SERVICE_CLUSTER          |create_cluster_example|                  YES
UPDATE_SWITCH                   |update_instance_example|                 YES
DELETE_SWITCH                   |update_instance_example|                 YES
CREATE_SWITCH_CLUSTER           |create_cluster_example|                  YES
DELETE_SWITCH_CLUSTER           |create_cluster_example|                  YES
UPDATE_CONFIG                   |update_instance_example|                 YES
DELETE_CONFIG                   |update_instance_example|                 YES
CREATE_CONFIG_CLUSTER           |create_cluster_example|                  YES
DELETE_CONFIG_CLUSTER           |create_cluster_example|                  YES
UPDATE_INFRA_CONFIG             |update_infra_config_example|             YES
DELETE_INFRA_CONFIG             |update_infra_config_example|             YES
ASSIGN_CLUSTER_LINK             |assign_cluster_link_example|             YES
DELETE_CLUSTER_LINK             |assign_cluster_link_example|             YES
UPDATE_ROUTE                    |update_route_example|                    YES
DELETE_ROUTE                    |update_route_example|                    YES
UPDATE_DEFAULT_ROUTE            |update_default_route|                    YES
DELETE_DEFAULT_ROUTE            |delete_default_route|                    YES
IMPORT_SERVICE                  |import_instances|                        YES
IMPORT_SWITCH                   |import_instances|                        YES
IMPORT_CONFIG                   |import_instances|                        YES
UPDATE_SERVICE_INFO             |update_service_info_example|             YES
UPDATE_CLUSTER_INFO             |update_service_info_example|             YES
GRANT_APPLICATION_AUTH          |update_application_auth_example|         YES
DISMISS_APPLICATION_AUTH        |update_application_auth_example|         YES
PROGRAM_UPDATE_ROUTE_STAGE      |program_update_route_stage|              YES
CREATE_TEAM                     |update_team_example|                     NO
DELETE_TEAM                     |update_team_example|                     NO
ARCHIVE_TEAM                    |update_team_example|                     NO
CREATE_APPLICATION              |update_application_example|              NO
DELETE_APPLICATION              |update_application_example|              NO
ARCHIVE_APPLICATION             |update_application_example|              NO
CREATE_USER                     |update_user_example|                     NO
DELETE_USER                     |update_user_example|                     NO
ARCHIVE_USER                    |update_user_example|                     NO
CHANGE_USER_PASSWORD            |update_user_example|                     NO
FORGOT_USER_PASSWORD            |update_user_example|                     NO
GRANT_HUSKAR_ADMIN              |update_user_example|                     NO
DISMISS_HUSKAR_ADMIN            |update_user_example|                     NO
OBTAIN_USER_TOKEN               |update_user_example|                     NO
GRANT_TEAM_ADMIN                |update_team_admin_example|               NO
DISMISS_TEAM_ADMIN              |update_team_admin_example|               NO
=============================== ========================================= =================

.. |update_instance_example| replace:: ``{"application_name": "foo.bar", "cluster_name": "Common", "key": "test_key"}``
.. |create_cluster_example| replace:: ``{"application_name": "foo.bar", "cluster_name": "Common"}``
.. |assign_cluster_link_example| replace:: ``{"application_name": "foo.bar", "cluster_name": "Common", "physical_name": "test"}``
.. |update_route_example| replace:: ``{"application_name": "foo.bar", "cluster_name": "Common", "destination_cluster_name": "test"}``
.. |update_service_info_example| replace:: ``{"application_name": "foo.bar", "cluster_name": "Common"}``
.. |update_application_auth_example| replace:: ``{"application_id": 1, "application_name": "foo.bar", "user_id": 1, "username": "san.zhang", "authority", "write"}``
.. |update_team_example| replace:: ``{"team_id": 1, "team_name": "foo"}``
.. |update_application_example| replace:: ``{"team_id": 1, "team_name": "foo", "application_id": 1, "application_name": "foo.bar"}``
.. |update_user_example| replace:: ``{"user_id": 1, "username": "san.zhang"}``
.. |update_team_admin_example| replace:: ``{"team_id": 1, "team_name": "foo", "user_id": 1, "username": "san.zhang"}``
.. |update_infra_config_example| replace:: ``{"infra_type": "redis", "scope_type": "idcs", "infra_name": "test", "scope_name": "vpc", "application_name": "foo.bar"}``
.. |update_default_route| replace:: ``{"application_name": "foo.bar", "cluster_name": "channel-stable-1", "ezone": "overall", "intent": "direct"}``
.. |delete_default_route| replace:: ``{"application_name": "foo.bar", "cluster_name": null, "ezone": "overall", "intent": "direct"}``
.. |import_instances| replace:: ``{"application_names": ["foo.bar"], "stored": true, "affected": 10, "overwrite": false}``
.. |program_update_route_stage| replace:: ``{"application_name": "foo.bar", "old_stage": "D", "new_stage": "C"}``

.. Note:: If the ``application_level`` is ``YES``, the event will be published to the webhook which ``webhook_type`` is ``0`` and has subscribe this event when the event is triggered.


Webhook API
-----------

The response schema of ``webhook`` instance schema.

================ =================== =============================
Name             Type                 Example
---------------- ------------------- -----------------------------
webhook_id       :js:class:`Number`  ``1``
webhook_type     :js:class:`Number`  ``0``
webhook_url      :js:class:`String`  ``"http://webhook.example.com"``
event_list       :js:class:`Array`   ``["CREATE_CONFIG_CLUSTER"]``
================ =================== =============================

.. autoflask:: huskar_api.wsgi:app
    :endpoints: api.webhook, api.webhook_instance, api.application_webhook
    :groupby: view
