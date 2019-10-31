.. _token:

Authorization and Token
=======================

A token is a credential of Huskar API. It provides information about
"who are you". All tokens are `JSON Web Token <https://jwt.io>`_ compatible.

For now, there are two types of token. The **user token** (human token)
points to a employee. The **app token** (application token) points to an
application.

The user tokens usually have an optional expiration time. But the app tokens
are always immortal.

Our deployment may deny requests to the bare API URL which use user tokens and
requests to the Web management console which use app tokens.

.. _how-to-use-token:

Using Token
-----------

The token could be placed in the request header :http:header:`Authorization`.
For example:

.. code-block:: sh

    HUSKAR_TOKEN="xxxx"
    curl http://example.com -H Authorization:$HUSKAR_TOKEN

.. _how-to-get-token:

Getting Token
-------------

There are two different APIs for getting :ref:`user-token` and
:ref:`app-token`.

.. _user-token:

User Token
~~~~~~~~~~

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.huskar_token

.. _app-token:

Application Token
~~~~~~~~~~~~~~~~~

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.application_token
