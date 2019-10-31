.. _user:

User Management
===============

.. _user_schema:

Basic Schema
------------

There is the response schema of user entities.

=============== =================== ================================
Name            Type                Example
--------------- ------------------- --------------------------------
id              :js:class:`Integer` ``10001``
username        :js:class:`String`  ``"san.zhang"``
email           :js:class:`String`  ``"san.zhang@example.com"``
is_active       :js:class:`Boolean` ``true``
is_admin        :js:class:`Boolean` ``false``
is_application  :js:class:`Boolean` ``false``
created_at      :js:class:`String`  ``"1993-05-01T12:00:00+08:00"``
updated_at      :js:class:`String`  ``"1993-05-01T12:00:00+08:00"``
=============== =================== ================================

Basic Management
----------------

You could create, modify and delete users via following API.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.user
   :groupby: view

Password Retrieval
------------------

The users have ability to reset their password by validating their email.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.password_reset
   :groupby: view

.. _site_admin:

Admin Management
----------------

The site admin has highest authority in Huskar API. The users could be granted
to site admin or dismissed from site admin via following API.

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.huskar_admin
   :groupby: view
