from __future__ import absolute_import

import json

from pytest import fixture

from huskar_api.models.tree.common import Path
from huskar_api.models.tree.extra import extra_handlers
from huskar_api.models.tree.watcher import TreeWatcher


@fixture
def watcher(hub):
    return TreeWatcher(hub)


def test_service_info_handler_with_invalid_path(
        zk, watcher, test_application_name):
    base_path = '/huskar/service/%s' % test_application_name
    instance_path = '%s/stable/192.168.10.1_8080' % base_path
    zk.create(
        instance_path, json.dumps({'ip': '191.168.10.1_8080'}),
        makepath=True)
    path = Path.parse(instance_path)

    update_handler = extra_handlers['service_info', 'update']
    assert update_handler(watcher, path) == {}

    all_handler = extra_handlers['service_info', 'all']
    assert all_handler(watcher, path) == {}
