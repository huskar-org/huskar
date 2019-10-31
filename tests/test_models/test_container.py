from __future__ import absolute_import

import datetime
import random

from pytest import fixture, mark, raises
from freezegun import freeze_time

from huskar_api.models import huskar_client
from huskar_api.models.container import ContainerManagement, is_container_id
from huskar_api.models.exceptions import NotEmptyError


@fixture
def container_id():
    cid = "".join(
        random.choice("0123456789abcdefghijklmnopqrstuvwxyz")
        for _ in range(64)
    )
    return cid


@fixture
def container_management(container_id):
    return ContainerManagement(huskar_client, container_id)


def test_is_container_id(container_id):
    assert is_container_id(container_id) is True
    assert is_container_id('b1B800dae3F') is False
    assert is_container_id('b1b800dae3fca57fcb429615ff3e0a7054') is False


@mark.xparametrize
def test_management_lookup(
        mocker, zk, container_management, container_id,
        present_path, result, warning):
    logger = mocker.patch(
        'huskar_api.models.container.management.logger', autospec=True)

    for path in present_path:
        zk.create(path % container_id, '', makepath=True)

    assert container_management.lookup() == [tuple(r) for r in result]

    if warning is None:
        logger.assert_not_called()
    else:
        warning_args = [w.format(container_id=container_id) for w in warning]
        logger.warning.assert_called_once_with(*warning_args)


@mark.xparametrize
def test_management_destroy(
        zk, container_management, container_id, monitor_client,
        present_path, vanished_path, has_metrics):
    for path in present_path:
        zk.create(path % container_id, '', makepath=True)

    container_management.destroy()

    for path in vanished_path:
        assert not zk.exists(path % container_id)

    if has_metrics:
        monitor_client.increment.assert_called_once_with('container.destroy')
    else:
        monitor_client.increment.assert_not_called()


@mark.xparametrize
def test_management_destroy_failed(
        zk, container_management, container_id, monitor_client,
        present_path, expected_path):
    for path in present_path:
        zk.create(path % container_id, '', makepath=True)

    with raises(NotEmptyError):
        container_management.destroy()

    for path in expected_path:
        assert zk.exists(path % container_id)

    monitor_client.increment.assert_not_called()


@mark.xparametrize
def test_management_register_to(
        mocker, zk, container_management, container_id,
        monitor_client, present_path, call_args, expected_children):
    for path in present_path:
        zk.create(path % container_id, '', makepath=True)

    for arg in call_args:
        container_management.register_to(arg['application'], arg['cluster'])

    for item in expected_children:
        actual_children = zk.get_children(item['path'] % container_id)
        assert sorted(actual_children) == sorted(item['children'])

    assert monitor_client.increment.mock_calls == (
        [mocker.call('container.register')] * len(call_args))


@mark.xparametrize
def test_management_register_to_failed(
        zk, container_management, container_id, monitor_client,
        call_arg, error_pattern):
    with raises(ValueError) as error:
        container_management.register_to(
            call_arg['application'], call_arg['cluster'])
    assert error.match(error_pattern)

    monitor_client.increment.assert_not_called()


@mark.xparametrize
def test_management_deregister_from(
        mocker, zk, container_management, container_id,
        monitor_client, present_path, call_args, expected_children):
    for path in present_path:
        zk.create(path % container_id, '', makepath=True)

    for arg in call_args:
        container_management.deregister_from(
            arg['application'], arg['cluster'])

    for item in expected_children:
        actual_children = zk.get_children(item['path'] % container_id)
        assert sorted(actual_children) == sorted(item['children'])

    assert monitor_client.increment.mock_calls == (
        [mocker.call('container.deregister')] * len(call_args))


def test_management_set_barrier(zk, container_management, container_id):
    path = '/huskar/container-barrier/%s' % container_id
    zk.delete(path, recursive=True)
    container_management.set_barrier()
    assert zk.exists(path)


def test_management_has_barrier(zk, container_management, container_id):
    assert not container_management.has_barrier()
    path = '/huskar/container-barrier/%s' % container_id
    zk.ensure_path(path)
    assert container_management.has_barrier()
    with freeze_time() as frozen_time:
        frozen_time.tick(datetime.timedelta(days=1.1))
        assert not container_management.has_barrier()


@mark.xparametrize
def test_management_vacuum_stale_barriers(zk):
    zk.delete('/huskar/container-barrier', recursive=True)
    zk.ensure_path('/huskar/container-barrier/foo')
    zk.ensure_path('/huskar/container-barrier/bar')

    vacuum = ContainerManagement.vacuum_stale_barriers(huskar_client)
    assert list(vacuum) == [('bar', False), ('foo', False)]
    assert zk.exists('/huskar/container-barrier/foo')
    assert zk.exists('/huskar/container-barrier/bar')

    with freeze_time() as frozen_time:
        frozen_time.tick(datetime.timedelta(hours=2))
        vacuum = ContainerManagement.vacuum_stale_barriers(huskar_client)
        assert list(vacuum) == [('bar', False), ('foo', False)]
        assert zk.exists('/huskar/container-barrier/foo')
        assert zk.exists('/huskar/container-barrier/bar')

        frozen_time.tick(datetime.timedelta(days=1))
        vacuum = ContainerManagement.vacuum_stale_barriers(huskar_client)
        assert list(vacuum) == [('bar', True), ('foo', True)]
        assert not zk.exists('/huskar/container-barrier/foo')
        assert not zk.exists('/huskar/container-barrier/bar')
