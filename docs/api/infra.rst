.. _infra:

Infrastructure
==============

.. _infra_config:

Universal Configuration
-----------------------

The universal infrastructure configuration follows `a spec <http://example.com/drafts/design/infra_key.md>`_ (a.k.a Naming Service).

It gives applications ability to discover and configure an infrastructure
client with a pre-defined key. For example::

    RedisClient redis = RedisClientRegistry.get('r100010')

There are high-level management API to register and configure infrastructure.
If you are a SDK developer, please don't use those management API in your
client-side code. Look for :ref:`long-polling` instead which provides stronger
guarantee.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.infra_config
   :groupby: view

.. _infra_config_downstream:

It is possible to look up the downstream applications of infrastructures also.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.infra_config_downstream
   :groupby: view
