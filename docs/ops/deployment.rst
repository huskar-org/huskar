.. _devops.deployment:

部署
====

依赖环境
--------

一个 Huskar API 实例的运行, 至少会依赖 ZooKeeper, MySQL 和 Redis 三种基础设施.

部署环境中需要准备好:

- 一个可访问的 ZooKeeper 集群
- 一个可访问的 MySQL 实例
- 一个可访问的 Redis 实例

自举
----

在一个全新的数据中心部署 Huskar API 时, 因为 Huskar API 本身也依赖 ZooKeeper 做配置管理,
故而会有 Bootstrap 问题. 一个可选的方案是通过 ``zkCli`` 访问 ZooKeeper 集群,
手动写入下述 Huskar API 赖以启动的配置,
然后启动 Huskar API 实例 (``PREFIX`` 为 ``/huskar/config/arch.huskar_api/overall``):

================================== ===========================================
Key                                Value
================================== ===========================================
``{PREFIX}/SECRET_KEY``             A random string
``{PREFIX}/DATABASE_URL``           mysql url
``{PREFIX}/REDIS_URL``              redis url
``{PREFIX}/HUSKAR_API_ZK_SERVICES`` zk hosts
================================== ===========================================

不过 *更加方便的一种方式* 是使用环境变量注入配置, 在 shell 中启动临时 Huskar API 实例:

.. code-block:: sh

   export HUSKAR_API_SECRET_KEY=<A random string>
   export HUSKAR_API_DATABASE_URL=mysql+pymysql://root@localhost:3306/huskar_api?charset=utf8
   export HUSKAR_API_REDIS_URL=redis://localhost:6379
   export HUSKAR_API_ZK_SERVICES=127.0.0.1:2181

   source .venv/bin/activate            # 进入 virtualenv
   ./manage.sh initadmin                # 创建管理员用户 (如果是全新的 MySQL 数据库)
   gunicorn -k gevent -b 0.0.0.0:5000 huskar_api.wsgi:app

临时实例启动后, 可发布 ``arch.huskar_fe`` 以启用 Web 面板, 使用面板将相关配置写入
``arch.huskar_api`` 之下. 完成后 `Ctrl-C` 关闭临时实例, 启动正式实例.

服务验证
--------

验证一个 Huskar API 实例是否正常部署的最简单方法是使用健康检查接口::

    curl http://localhost:5000/api/health_check

健康检查接口返回 HTTP 200 说明至少 Gunicorn 实例正常启动, ZooKeeper 可以正常连接.
如果还需要验证 MySQL 等其他设施是否配置正确, 则需要通过功能 API 或访问 Huskar
Web 面板.

.. _huskar-api: http://example.com/huskar-api
.. _huskar-fe: http://example.com/huskar-fe
