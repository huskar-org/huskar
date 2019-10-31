#!/usr/bin/env python

from __future__ import absolute_import, print_function

from sqlalchemy import func
from huskar_sdk_v2.consts import OVERALL

from huskar_api import settings
from huskar_api.models import DBSession, cache_manager
from huskar_api.models.auth import ApplicationAuth
from huskar_api.models.catalog import ServiceInfo
from huskar_api.models.dataware.zookeeper import switch_client, config_client


def check_settings():
    for intent in settings.ROUTE_INTENT_LIST:
        cluster_name = settings.ROUTE_DEFAULT_POLICY.get(intent)
        assert cluster_name, 'Incomplete ROUTE_DEFAULT_POLICY: %s' % intent
        ServiceInfo.check_default_route_args(OVERALL, intent, cluster_name)


def check_mysql():
    if is_minimal_mode():
        print('minimal mode detected')
        return
    db = DBSession()
    assert db.query(func.count(ApplicationAuth.id)).scalar(), 'mysql not ok'


def check_zookeeper():
    value_cluster = config_client.get(
        settings.APP_NAME,
        settings.CLUSTER,
        'SECRET_KEY')
    value_overall = config_client.get(
        settings.APP_NAME,
        'overall',
        'SECRET_KEY')
    assert any((value_cluster, value_overall)), 'zk not ok'


def check_redis():
    if is_minimal_mode():
        print('minimal mode detected')
        return
    client = cache_manager.make_client(raw=True)
    client.set('huskar_service_check', 'hello')
    assert client.get('huskar_service_check') == 'hello', 'redis not ok'


def is_minimal_mode():
    value_cluster = switch_client.get(
        settings.APP_NAME,
        settings.CLUSTER,
        'enable_minimal_mode')
    value_overall = switch_client.get(
        settings.APP_NAME,
        'overall',
        'enable_minimal_mode')
    return float(value_cluster or value_overall or 0) > 0


def main():
    check_settings()
    check_mysql()
    check_zookeeper()
    check_redis()


if __name__ == '__main__':
    main()
