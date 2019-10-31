from __future__ import absolute_import

import datetime

from pytest import fixture, mark
from freezegun import freeze_time

from huskar_api.models import huskar_client
from huskar_api.models.container import ContainerManagement
from huskar_api.models.instance import InstanceManagement
from ..utils import assert_response_ok


@fixture
def container_id(faker):
    return faker.uuid4()


@fixture
def container_management(container_id):
    return ContainerManagement(huskar_client, container_id)


@fixture
def instance_list(request, container_id):
    instance_list = []
    for application_name, cluster_name in request.param:
        im = InstanceManagement(huskar_client, application_name, 'service')
        instance, _ = im.get_instance(cluster_name, container_id)
        instance.data = ''
        instance.save()
        instance_list.append(instance)
    return instance_list


@mark.xparametrize
def test_list_registry(
        client, container_id, container_management, test_token, minimal_mode,
        preset, result):
    container_management.set_barrier()
    for application_name, cluster_name in preset:
        container_management.register_to(application_name, cluster_name)

    url = '/api/_internal/tools/container/registry/%s' % container_id
    r = client.get(url, headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.json['data'] == {'registry': result, 'barrier': True}

    with freeze_time() as frozen_time:
        frozen_time.tick(datetime.timedelta(days=1.1))
        for _ in ContainerManagement.vacuum_stale_barriers(huskar_client):
            pass

    r = client.get(url, headers={'Authorization': test_token})
    assert_response_ok(r)
    assert r.json['data']['barrier'] is False


def test_list_registry_without_auth(client, container_id):
    url = '/api/_internal/tools/container/registry/%s' % container_id
    r = client.get(url)
    assert r.status_code == 401, r.data
    assert r.json['status'] == 'Unauthorized'


@mark.parametrize('registry,instance_list', [
    ([], []),
    ([('base.foo', 'alpha_stable')],
     [('base.foo', 'alpha_stable')]),
    ([('base.foo', 'alpha_stable'), ('base.bar', 'alpha_dev')],
     [('base.foo', 'alpha_stable')]),
    ([('base.foo', 'alpha_stable'), ('base.bar', 'alpha_dev')],
     [('base.foo', 'alpha_stable'), ('base.bar', 'alpha_dev')]),
], indirect=['instance_list'])
def test_deregister(
        client, zk, container_management, container_id, admin_token,
        registry, instance_list):
    for application_name, cluster_name in registry:
        container_management.register_to(application_name, cluster_name)
    assert not container_management.has_barrier()

    url = '/api/_internal/tools/container/registry/%s' % container_id
    r = client.delete(url, headers={'Authorization': admin_token})
    assert_response_ok(r)

    for instance in instance_list:
        assert not zk.exists(instance.path)
    assert not zk.exists('/huskar/container/%s' % container_id)
    assert container_management.has_barrier()


def test_deregistry_without_auth(client, container_id, test_token):
    url = '/api/_internal/tools/container/registry/%s' % container_id
    r = client.delete(url)
    assert r.status_code == 401, r.data
    assert r.json['status'] == 'Unauthorized'

    url = '/api/_internal/tools/container/registry/%s' % container_id
    r = client.delete(url, headers={'Authorization': test_token})
    assert r.status_code == 400, r.data
    assert r.json['status'] == 'NoAuthError'


def test_deregistry_with_conflict(
        client, mocker, container_management, container_id, admin_token):
    container_management.register_to('base.foo', 'alpha_stable')
    container_management.register_to('base.bar', 'alpha_dev')

    lookup = mocker.patch.object(ContainerManagement, 'lookup')
    lookup.return_value = [('base.foo', 'alpha_stable')]

    url = '/api/_internal/tools/container/registry/%s' % container_id
    r = client.delete(url, headers={'Authorization': admin_token})
    assert r.status_code == 409, r.data
    assert r.json['status'] == 'Conflict'
    assert r.json['message'] == (
        'Container %s is still registering new instance' % container_id)
