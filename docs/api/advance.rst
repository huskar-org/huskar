.. _advance:

Advance Management
==================

.. _service_link:

Service Link
------------

The **Service Link** is a simple way to redirect traffic of a cluster to
another one in the side of service provider.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.service_link
   :groupby: view


.. _service_route:

Service Route
-------------

The **Service Route** is another way to manage the traffic of clusters. You
could demand to redirect traffic which going to specific destination
application, in the side of service consumer.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.service_route
   :groupby: view

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.service_default_route
   :groupby: view


.. _service_info:

Service Info
------------

The API of **Service Info** is used for the management of application-scope and
cluster-scope information, which is read by sidecar for health check.

The ``cluster_name`` is an optional component of URL. If you don't specify it,
the API works on application-scope. Otherwise the API works on cluster-scope.

Instead of using the API always, we recommend you consider trying the
`Web Console`_.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.service_info,api.cluster_info
   :groupby: view

.. _`Web Console`: https://example.com/application/foo.bar/service?info
