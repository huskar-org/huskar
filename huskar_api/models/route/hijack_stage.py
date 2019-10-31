from __future__ import absolute_import

from flask import json

from huskar_sdk_v2.consts import CONFIG_SUBDOMAIN

from huskar_api.settings import APP_NAME


__all__ = ['lookup_route_stage']


def lookup_route_stage():
    from huskar_api.models import huskar_client
    from huskar_api.models.instance import InstanceManagement

    stage_table = {}
    im = InstanceManagement(huskar_client, APP_NAME, CONFIG_SUBDOMAIN)
    cluster_list = im.list_cluster_names()
    for cluster_name in cluster_list:
        instance, _ = im.get_instance(cluster_name, 'ROUTE_HIJACK_LIST')
        data = json.loads(instance.data) if instance.data else {}
        for application_name, stage in data.items():
            t = stage_table.setdefault(application_name, {})
            t[cluster_name] = stage
    return stage_table
