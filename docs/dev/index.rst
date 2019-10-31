Internal API
============

Common
------

.. automodule:: huskar_api.models
   :members:

.. automodule:: huskar_api.models.znode
   :members:

Authorization
-------------

.. automodule:: huskar_api.models.auth.application
   :members:

.. automodule:: huskar_api.models.auth.team
   :members:

.. automodule:: huskar_api.models.auth.user
   :members:

.. automodule:: huskar_api.models.auth.session
   :members:

Audit Log
---------

We have defined three level of audit log, ``site``, ``team`` and ``application``,
the content of audit log is a ``Dict``, it must have a field named ``action_data`` to
be used to describe the action of this audit log.

.. Note:: If the level is ``application``, there must be a ``string`` field ``application_name``
          or a ``list`` field ``application_names`` in the value of ``action_data``.


.. automodule:: huskar_api.models.audit.audit
   :members:

.. automodule:: huskar_api.models.audit.action
   :members:

.. automodule:: huskar_api.models.audit.index
   :members:

.. automodule:: huskar_api.models.audit.const
   :members:

Tree Watch
----------

.. automodule:: huskar_api.models.tree.hub
   :members:

.. automodule:: huskar_api.models.tree.holder
   :members:

.. automodule:: huskar_api.models.tree.watcher
   :members:

.. automodule:: huskar_api.models.tree.common
   :members:

Service Management
------------------

.. automodule:: huskar_api.models.catalog
   :members:

.. automodule:: huskar_api.models.instance
   :members:

.. automodule:: huskar_api.models.manifest
   :members:

.. automodule:: huskar_api.models.comment
   :members:
