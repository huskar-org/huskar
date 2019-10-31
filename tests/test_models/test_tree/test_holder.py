from __future__ import absolute_import

import json
import random

from pytest import fixture, raises
from gevent import spawn, joinall, sleep
from gevent.event import Event
from kazoo.protocol.connection import _CONNECTION_DROP
from kazoo.recipe.cache import TreeEvent

from huskar_api.models import huskar_client
from huskar_api.models.tree import TreeHub
from huskar_api.models.tree.holder import TreeHolder
from huskar_api.models.exceptions import TreeTimeoutError
from tests.utils import assert_semaphore_is_zero


@fixture
def logger(mocker):
    return mocker.patch('huskar_api.models.tree.holder.logger', autospec=True)


def test_start(test_application_name):
    hub = TreeHub(huskar_client)
    holder = hub.get_tree_holder(test_application_name, 'config')

    # It is okay to get it again from holder
    assert hub.get_tree_holder(test_application_name, 'config') is holder

    # It will be initialized finally
    holder.block_until_initialized(5)


def test_start_with_throttle(faker):
    prefix = 'test_start_with_throttle.%s' % faker.uuid4()[:8]

    class RoughTreeHolder(TreeHolder):
        def dispatch_signal(self, event):
            if event.event_type == TreeEvent.INITIALIZED:
                sleep(random.random())  # 0 ~ 1s
            return TreeHolder.dispatch_signal(self, event)

    def start_concurrently(startup_max_concurrency):
        hub = TreeHub(huskar_client, startup_max_concurrency)
        hub.tree_holder_class = RoughTreeHolder
        execution_order = []

        def handle_request(seq):
            application_name = '%s.%d' % (prefix, seq)
            holder = hub.get_tree_holder(application_name, 'config')
            holder.block_until_initialized(5)
            execution_order.append(seq)
            hub.release_tree_holder(application_name, 'config')

        joinall([spawn(handle_request, i) for i in range(5)])
        return execution_order, hub.throttle

    execution_order, semaphore = start_concurrently(1)
    assert sorted(execution_order) == execution_order
    assert_semaphore_is_zero(semaphore, 1)

    execution_order, semaphore = start_concurrently(3)
    assert sorted(execution_order) != execution_order
    assert_semaphore_is_zero(semaphore, 3)


def test_start_timeout_with_throttle(test_application_name, zk):
    path = '/huskar/config/%s/overall/TEST_KEY' % test_application_name
    zk.create(path, value=b'{}', makepath=True)

    condition = Event()

    class ConditionTreeHolder(TreeHolder):
        def dispatch_signal(self, event):
            if event.event_type == TreeEvent.INITIALIZED:
                condition.wait()
            return TreeHolder.dispatch_signal(self, event)

    hub = TreeHub(huskar_client, 1)
    hub.tree_holder_class = ConditionTreeHolder
    assert not hub.throttle.locked()

    holder = hub.get_tree_holder(test_application_name, 'config')
    assert hub.throttle.locked()

    with raises(TreeTimeoutError):
        holder.block_until_initialized(1)
    assert not hub.throttle.locked()

    hub.release_tree_holder(test_application_name, 'config')
    assert not hub.throttle.locked()

    condition.set()
    assert holder.initialized.wait(1)
    assert holder._started
    assert holder._closed
    assert_semaphore_is_zero(hub.throttle, 1)


def test_report_errors(mocker, logger, holder):
    exc = Exception()
    root = mocker.patch.object(holder.cache, '_root')
    root.on_deleted.side_effect = exc

    holder.cache.close()
    logger.exception.assert_called_once_with(exc)


def test_connective_events(mocker, logger, holder):
    is_reached = Event()
    is_reached.clear()
    holder.cache.listen(lambda event: is_reached.set())

    # Learned from the test case of Kazoo
    huskar_client.client._call(_CONNECTION_DROP, None)

    is_reached.wait(3)
    logger.info.assert_has_calls([mocker.call(
        'Connective event %s happened on %s', 'SUSPENDED', holder.path)])
    huskar_client.client._live.wait(3)


def test_ignore_unneeded_events(monitor_client, holder):
    event = TreeEvent.make(TreeEvent.INITIALIZED, None)
    holder.dispatch_signal(event)
    assert holder.initialized.is_set()
    monitor_client.increment.assert_called()
    monitor_client.increment.reset_mock()
    holder.dispatch_signal(event)
    assert holder.initialized.is_set()
    monitor_client.increment.assert_not_called()
    holder.cache.close()


def test_list_instance_nodes(zk, test_application_name, holder, hub):
    assert set(holder.list_instance_nodes()) == set()

    is_reached = Event()
    is_reached.clear()

    @holder.tree_changed.connect_via(holder)
    def reach(sender, event):
        print event
        is_reached.set()

    def wait_and_reset():
        is_reached.wait()
        is_reached.clear()

    base_path = '/huskar/config/%s/stable' % test_application_name

    zk.create(base_path, makepath=True)
    wait_and_reset()

    zk.create('%s/DB_URL' % base_path, b'foo')
    wait_and_reset()

    zk.create('%s/DB_URI' % base_path, b'bar')
    wait_and_reset()

    assert set(holder.list_instance_nodes()) == {
        (('config', test_application_name, 'stable', 'DB_URL'), 'foo'),
        (('config', test_application_name, 'stable', 'DB_URI'), 'bar'),
    }

    zk.delete('%s/DB_URI' % base_path)
    wait_and_reset()

    assert set(holder.list_instance_nodes()) == {
        (('config', test_application_name, 'stable', 'DB_URL'), 'foo'),
    }

    node = holder.cache._root._children['stable']._children['DB_URL']
    node._data = None
    assert set(holder.list_instance_nodes()) == set()


def test_list_service_info(zk, test_application_name, service_holder, hub):
    is_reached = Event()
    is_reached.clear()

    @service_holder.tree_changed.connect_via(service_holder)
    def reach(sender, event):
        print event
        is_reached.set()

    def wait_and_reset():
        is_reached.wait()
        is_reached.clear()

    base_path = '/huskar/service/%s' % test_application_name
    zk.create('%s/stable' % base_path, makepath=True)
    wait_and_reset()
    assert dict(service_holder.list_service_info()) == {
        'overall': {}, 'stable': {}}

    zk.set(base_path, json.dumps({'info': {'balance_policy': 'RoundRobin'}}))
    wait_and_reset()
    assert dict(service_holder.list_service_info(['overall'])) == {
        'overall': {'balance_policy': {'value': '"RoundRobin"'}}}

    zk.set('%s/stable' % base_path, json.dumps({
        'info': {'dict': {"port": 8080}}}))
    wait_and_reset()
    assert dict(service_holder.list_service_info()) == {
        'overall': {'balance_policy': {'value': '"RoundRobin"'}},
        'stable': {'dict': {'value': '{"port": 8080}'}}}

    assert dict(service_holder.list_service_info(['stable'])) == {
        'stable': {'dict': {'value': '{"port": 8080}'}}}

    zk.create(
        '%s/overall' % base_path, json.dumps(
            {'info': {'balance_policy': 'Random'}}),
        makepath=True)
    wait_and_reset()
    assert dict(service_holder.list_service_info()) == {
        'overall': {'balance_policy': {'value': '"RoundRobin"'}},
        'stable': {'dict': {'value': '{"port": 8080}'}}}

    zk.set(
        '%s/stable' % base_path, '233')
    wait_and_reset()
    assert dict(service_holder.list_service_info(['stable'])) == {'stable': {}}
