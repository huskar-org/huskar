from __future__ import absolute_import

import datetime

import pytest
import freezegun

from huskar_api import settings
from huskar_api.models import huskar_client
from huskar_api.models.catalog import ServiceInfo, ClusterInfo
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.manifest import application_manifest
from huskar_api.models.exceptions import OutOfSyncError
from huskar_api.scripts.vacuum import (
    vacuum_empty_clusters, vacuum_stale_barriers)


@pytest.fixture
def format_text(faker):
    variables = {
        'test_application': faker.uuid4()[:8],
        'base_path': faker.uuid4()[:8],
    }
    return lambda text: text.format(**variables)


@pytest.fixture
def zk_safe(mocker, zk, format_text):
    """Protects the huskar_client from "start" / "stop" outside."""
    base_path = format_text('/huskar_{base_path}')

    mocker.patch.object(huskar_client, 'base_path', base_path)
    mocker.patch.object(
        ServiceInfo, 'PATH_PATTERN',
        base_path + '/{type_name}/{application_name}')
    mocker.patch.object(
        ClusterInfo, 'PATH_PATTERN',
        base_path + '/{type_name}/{application_name}/{cluster_name}')
    mocker.patch.object(application_manifest, 'as_list', return_value=[
        format_text('{test_application}'), 'bar', 'foo ', 'black_appid'])
    huskar_client.ensure_path('')

    mocker.patch.object(
        settings, 'AUTH_APPLICATION_BLACKLIST', ['black_appid'])

    try:
        yield zk
    finally:
        zk.delete(base_path, recursive=True)


@pytest.mark.xparametrize('test_cluster_data')
def test_vacuum_empty_clusters(
        zk_safe, format_text, before, should_exist, should_not_exist):
    for item in before:
        zk_safe.create(format_text(item['path']), item['data'], makepath=True)

    vacuum_empty_clusters()

    for item in should_exist:
        assert zk_safe.exists(format_text(item['path']))
    for item in should_not_exist:
        assert not zk_safe.exists(format_text(item['path']))


@pytest.mark.xparametrize('test_cluster_data')
def test_vacuum_empty_clusters_oos(
        mocker, zk_safe, format_text, before, should_exist, should_not_exist):
    for item in before:
        zk_safe.create(format_text(item['path']), item['data'], makepath=True)

    mocker.patch.object(
        InstanceManagement, 'delete_cluster', side_effect=OutOfSyncError)

    vacuum_empty_clusters()

    for item in should_exist + should_not_exist:
        assert zk_safe.exists(format_text(item['path']))


def test_vacuum_stale_barriers(zk):
    zk.delete('/huskar/container-barrier', recursive=True)
    zk.ensure_path('/huskar/container-barrier/foo')
    zk.ensure_path('/huskar/container-barrier/bar')

    with freezegun.freeze_time() as frozen_time:
        vacuum_stale_barriers()
        assert zk.exists('/huskar/container-barrier/foo')
        assert zk.exists('/huskar/container-barrier/bar')

        frozen_time.tick(datetime.timedelta(days=1.5))

        vacuum_stale_barriers()
        assert not zk.exists('/huskar/container-barrier/foo')
        assert not zk.exists('/huskar/container-barrier/bar')
