.. _audit:

Audit Log
=========

You can view the audit log via the `Web Console`_. It is based on following
API.

.. _`Web Console`: https://example.com/audit
.. _audit_schema:

Schema
------

There is the response schema of audit log entities.

=============== =================== =====================================
Name            Type                Example
--------------- ------------------- -------------------------------------
id              :js:class:`Integer` ``10001``
user            :js:class:`Object`  :ref:`User Schema <user_schema>`
remote_addr     :js:class:`String`  ``"10.0.0.1"``
action_name     :js:class:`String`  ``"CREATE_TEAM"``
action_data     :js:class:`String`  ``"{"team_name": "base"}"``
created_at      :js:class:`String`  ``"1993-05-01T12:00:00+08:00"``
rollback_to     :js:class:`Object`  ``null`` or
                                    :ref:`Audit Log Schema<audit_schema>`
=============== =================== =====================================


API
---

.. autoflask:: huskar_api.wsgi:app
   :endpoints: api.audit_site,api.audit_team,api.audit_application
   :groupby: view
