.. _long-polling:

Event Subscription (Long Polling)
=================================

Quick Start
-----------

You may hope to have a quick show of service discovery without installing a
bunch of libraries. It is possible to be done with ``curl``::

   $ MY_HUSKAR_URL=http://api.huskar.example.com
   $ MY_HUSKAR_TOKEN=...  # token of "foo.bar"
   $ MY_CLUSTER_NAME=test_cluster
   $ MY_ARGS='{"service":{"foo.test":["direct"], "foo.bar":["direct"]}}'
   $ curl -X POST $MY_HUSKAR_URL/api/data/long_poll \
         -H Authorization:$MY_HUSKAR_TOKEN \
         -H Content-Type:application/json \
         -H X-SOA-Mode:route \
         -H X-Cluster-Name:$MY_CLUSTER_NAME \
         -d "$MY_ARGS"

Don't close the terminal window now, and register or deregister service
instances on the web management console of Huskar::

    https://example.com/application/foo.bar/service

You will see the changes are printed in your terminal window.

Basic API
---------

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.long_polling

.. _long-polling-message-types:


Subscription Types
------------------

Huskar uses a JSON object as the subscription payload. It is better to put all topic data you want to subscribe in
the JSON. The most obvious benefit of this is that the client can only hold one connection with Huskar to avoid
waste of resources. The supported topic types are described below:

Baisc Subscription Type
~~~~~~~~~~~~~~~~~~~~~~~

"Config" Type
^^^^^^^^^^^^^

The ``config`` type is used for subscribe data changes of ``config`` instance, see also :ref:`config`

"Switch" Type
^^^^^^^^^^^^^

The ``switch`` type is used for subscribe data changes of ``switch`` instance. see also :ref:`switch`

"Service" Type
^^^^^^^^^^^^^^

The 'service' type is used for subscribe data chagnes of ``service`` instance. see also :ref:`service`


Extra Subscription Type
~~~~~~~~~~~~~~~~~~~~~~~

"Service Info" Type
^^^^^^^^^^^^^^^^^^^

Different with others, ``service_info`` type is used on application and cluster level instead of instance level.
For being backward compatible, the data schema of ``service_info`` are kept the same as other types, it should
be mentioned that the ``overall`` cluster of ``service_info`` are used to represent the application level, so
any broadcasted data of ``overall`` cluster is about the changes of application level.

The subscription payload as shown in the example below::

{"service_info": ["overall", "stable"]}


Message Types
-------------

"Ping" Message
~~~~~~~~~~~~~~

The ``ping`` message appears when the connection is idle. It looks like::

    {"message": "ping", "body": {}}

It could be ignored it safety in your client implementation.

"All" Message
~~~~~~~~~~~~~

The ``all`` message appears in following situation:

- The connection is initial and the ``trigger`` parameter is ``1``
- The connection is established but a subscribed cluster is overlaid by
  symlink changes or route changes.

This kind of message includes all data matched the request. It looks like::

    {
        "body": {
            "service": {
                "foo.bar": {
                    "overall.alta1": {
                        "192.186.0.1_5000": {
                            "value": "{\"ip\": \"192.186.0.1\", \
                                       \"state\": \"up\", \
                                       \"port\": {\"main\": 15100}}"
                        }
                    }
                }
            },
            "config": {
                "foo.bar": {
                    "overall.alta1": {
                        "DB_URL": {
                            "value": "mysql+pymysql://"
                        }
                    }
                }
            }
        },
        "message": "all"
    }

"Update" Message
~~~~~~~~~~~~~~~~

The ``update`` message appears after data creating or updating. It looks like::

    {
        "body": {
            "switch": {
                "foo.bar": {
                    "overall.alta1": {"enable_user": {"value": "50.2"}}
                }
            }
        },
        "message": "update"
    }

The ``value`` will always be strings. For service instances, the ``value``
could be parsed into a JSON object.

"Delete" Message
~~~~~~~~~~~~~~~~

The ``delete`` message appears after data deleting. It looks like::

    {
        "body": {
            "switch": {
                "foo.bar": {
                    "overall.alta1": {"get_city": {"value": null}}
                }
            }
        },
        "message": "delete"
    }

The ``value`` will always be ``null``.
