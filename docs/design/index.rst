.. _design:

设计
====

概述
----

服务定位
~~~~~~~~

Huskar API 的定位是一个 **高可用** 、**弱状态** 的服务治理 **中间件**,
功能涵盖服务发现、服务注册和配置管理。一致性决议和持久化不是 Huskar API 的目标,
这部分职责由 ZooKeeper 承担。

除 ZooKeeper 外, Huskar API 依赖的其他上游服务均可被降级, 包括 MySQL, Redis 等。

ZooKeeper Schema
----------------

Huskar API 的核心数据存储在 ZooKeeper 中, 数据 Schema 如下::

    /huskar/service/${application_name}/${cluster_name}/${key}
    /huskar/switch/${application_name}/${cluster_name}/${key}
    /huskar/config/${application_name}/${cluster_name}/${key}

以上 Schema 中涉及的变量解释如下:

- **application_name** 标识一个服务的 APPID, 格式为 ``domain.name``, 全局唯一。
  例: ``foo.huskar_api``
- **cluster_name** 集群名, 用于区分 IDC (ezone) 和集群。
  服务调用的流量以集群的粒度路由，一个集群包含了多个服务实例。
  例: ``altb1-channel-stable-1``
- **key** 服务实例或开关项、配置项的名字。例: ``10.0.0.1_5000`` 或 ``DB_URL``

权限机制
--------

出于权限管理考量, 每个 application 都会从属于一个 team, 这不影响 application
名字的全局唯一性 —— 即使在不同的 team 也不允许出现同名的 application。

每个 team 的管理员拥有对 team 内所有 application 的读取和写入权限, 并且有权在
team 内新建或删除 application。此外每个 application 也都可以单独授权用户或另一个
application 的读取或写入权限。

全站管理员则是一种特权身份, 除了拥有所有 team 的管理员的权限外, 还能添加用户、
删除用户、任命和解除 team 管理员。

用户的身份由 JWT token 标定, 目前从系统获取的 token 会根据是 user 还是
application 给予不同的过期时间。默认 user token 一个月过期, application token
永不过期。

详见:

- :ref:`team`
- :ref:`application`
- :ref:`token`
- :ref:`site_admin`

.. _traffic_control:

流量治理
--------

Huskar API 的流量治理方式包括软链和路由。如上文所述,
这两种治理方式都作用于集群维度。

.. _traffic_control_symlink:

软链
~~~~

软链, 又名 **service link** 或 **cluster link**, 是指服务提供方 (callee
application) 将某个集群的流量重定向到另一个集群, 后者被称为 **物理集群 (physical
cluster)** 。

软链的实现方式是替换服务发现时集群内的实例列表。当集群被设置了软链, Huskar API
对于该集群的请求或者事件订阅均以物理集群的实例为准。

软链只影响服务发现，不影响服务注册。\
添加或者删除服务实例的操作不会被重定向到物理集群。

软链不允许存在层级, 如果一次重定向之后, 物理集群上仍然有下一级软链信息,
Huskar API 不会再次重定向, 而是简单地忽略物理集群上的软链设置。

管理软链的 API 可以参考 :ref:`service_link`\ 。

.. _traffic_control_route:

路由
~~~~

.. _traffic_control_route_intro:

概述
^^^^

路由, 又名 **SOA route**, 是一种类似软链的集群流向重定向方式。不同于软链的是,
路由通过在服务调用方 (caller application) 添加路由规则, 而只将匹配四元组

- ``consumer_application_name``
- ``consumer_cluster_name``
- ``provider_application_name``
- ``intent``

的服务发现请求或者事件订阅转发到指定的 ``provider_cluster_name`` 。

其中 ``intent`` 为路由规则的类别标记, 目前只能是选择:

- ``direct`` - 意图订阅服务的可直连实例

类似软链, 路由只影响服务发现, 不影响服务注册。但又不同于软链, 路由到目标集群之后,
如果目标集群上有软链设置, 软链设置会生效, 服务被再次重定向到物理集群。
之后物理集群上如果还有软链或路由信息, 遵循软链解析的规则, 这些信息会被忽略。

换言之, 路由的目标集群可以有软链, 但路由的目标集群不会被再次路由;
而软链的目标集群即已经是最终的物理集群, 上面既不能再次软链, 也不能路由。

Web 面板
--------

面板介绍
~~~~~~~~

Huskar API 仅提供 HTTP API 供 SDK 接入。Web 面板是纯前端项目, 属于典型 SPA,
以反向代理的方式同源使用 Huskar API 的子集。
