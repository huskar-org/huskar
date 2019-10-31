.. _application:

Application Management
======================

An application of Huskar is an organization for containing service, switch and
config. It includes a name (a.k.a appid) and a related team.

The team admin have default authority (``read`` and ``write``) on applications
inside their team. You could see :ref:`team` also.

There is a serial of API to view and manage applications.

Basic Management
----------------

.. _application_list:

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.application
   :groupby: view

.. _application_item:

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.application_item
   :groupby: view

.. _application_auth:

Authority Management
--------------------

There are two types of authority, ``read`` and ``write``.

The ``read`` authority allows people to read secret area of applications,
including *switch*, *config* and *audit log*. All authenticated users can read
public area, including *service registry* for now, without any authority.

The ``write`` authority is required for creating or updating anything in
public area and secret area, unless the authenticated user is a
:ref:`site admin <site_admin>` or :ref:`team admin <team_admin>`.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.application_auth
   :groupby: view
