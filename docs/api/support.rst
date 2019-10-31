.. _support:

Internal Support
================

The following APIs are designed for internal usage. We use them to support
some special situation.

Please **do not use them** if you don't know what are them.

Internal API
-----------------

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.whoami
   :groupby: view

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.team_application_token
   :groupby: view

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.internal_container_registry
   :groupby: view

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.internal_route_program
   :groupby: view

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.service_weight
   :groupby: view

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.well_known_common
   :groupby: view

OPS Internal API
----------------

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.internal_blacklist
   :groupby: view
