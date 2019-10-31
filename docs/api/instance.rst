.. _instance:

Instance Management
===================

An instance may be a **service**, **switch** or **config**.

.. _service:

Service Management
------------------

For service registry and discovery, there is API suite for it. It is like
the API of :ref:`switch` but a bit of difference will exist.


There is the response schema of ``service`` instance schema.

================ =================== ================== ================
Name             Type                 Spec              Example
---------------- ------------------- ------------------ ----------------
application      :js:class:`String`  |appid_spec|       ``"foo.test"``
cluster          :js:class:`String`  |cluster_spec|     ``"overall"``
key              :js:class:`String`  `-`                ``"127.0.0.1_8080"``
value            :js:class:`Object`  `-`                ``{"ip": "127.0.0.1", "state": "up", "port": {"main": 5000, "back": 8000}}``
meta             :js:class:`Object`  `-`                ``{"created": 1502696650154}``
================ =================== ================== ================

The ``value`` field shcema in service instance

================ =================== ================
Name             Type                 Example
---------------- ------------------- ----------------
ip               :js:class:`String`   ``"127.0.0.1"``
port             :js:class:`Object`  ``{"main": 500, "back": 8000}``
state            :js:class:`String`   ``"up"``
meta             :js:class:`Object`   ``{"protocol": "http"}``
================ =================== ================

.. note:: Please make sure the data format of `value` does match the schema we define uppon.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.service
   :groupby: view

Service Registry
----------------

There is a lightweight API for SDK users also. It is recommended to use it for
registering a service itself.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.service_registry
   :groupby: view

.. _switch:
.. _config:

Switch and Config
-----------------

The switch and config instances are managed by similar API. The only difference
is that the switch usually accept number values in ``0`` to ``100`` range,
which means the probability of pass. This behavior is implemented in client
side.


There is the response schema of ``switch and config`` instance entities.

================ ================== ============== ===========
Name             Type               Spec           Example
---------------- ------------------ -------------- -----------
application      :js:class:`String` |appid_spec|   ``"foo.test"``
cluster          :js:class:`String` |cluster_spec| ``"overall"``
key              :js:class:`String` `-`            ``"test"``
value            :js:class:`String` `-`            ``"test"``
meta             :js:class:`Object` `-`            ``{"created":1502696650154,"last_modified":1502696650154,"version":1}``
comment          :js:class:`String` `-`            ``"for test"``
================ ================== ============== ===========

The ``meta`` filed was given some meta information of the key:

============= =================== =============
Name          Type                Examle
------------- ------------------- -------------
created       :js:class:`Number`  1502696650154
last_modified :js:class:`Number`  1502696650154
version       :js:class:`Number`  1
============= =================== =============


.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.switch,api.config
   :groupby: view

.. _instance_batch:

Batch Operation
---------------

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.service_batch,api.switch_batch,api.config_batch
   :groupby: view

.. |appid_spec| replace:: ``^[a-zA-Z0-9][a-zA-Z0-9_\-.]{0,126}[a-zA-Z0-9]``
.. |cluster_spec| replace:: ``^(?!^\.+$)(a-zA-Z0-9_\-.){1,64}$``

