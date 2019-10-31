from __future__ import absolute_import

import json

from pytest import fixture
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN

from huskar_api import settings
from huskar_api.models import huskar_client
from huskar_api.models.route import RouteManagement
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.tree.watcher import TreeWatcher
from huskar_api.models.route.utils import make_route_key


@fixture
def route_management(test_application_name):
    return RouteManagement(
        huskar_client, test_application_name, 'alta1-stable')


@fixture
def instance_management(test_application_name):
    return InstanceManagement(
        huskar_client, test_application_name, SERVICE_SUBDOMAIN)


@fixture
def set_route(route_management):
    def _set_route(application_name, cluster_name, intent=None):
        service_info = route_management.make_service_info()
        service_info.add_dependency(
            application_name, route_management.cluster_name)
        service_info.save()

        cluster_info = route_management.make_cluster_info(application_name)
        cluster_info.set_route(
            make_route_key(route_management.application_name, intent),
            cluster_name)
        cluster_info.save()

    return _set_route


@fixture
def watcher(hub):
    return TreeWatcher(hub)


@fixture
def update_legacy_application_list():
    try:
        yield settings.update_legacy_application_list
    finally:
        settings.update_legacy_application_list([])


@fixture
def update_route_from_cluster_blacklist():
    try:
        yield settings.update_route_from_cluster_blacklist
    finally:
        settings.update_route_from_cluster_blacklist({})


@fixture
def update_route_dest_cluster_blacklist():
    try:
        yield settings.update_route_dest_cluster_blacklist
    finally:
        settings.update_route_dest_cluster_blacklist({})


def test_watch(watcher, test_application_name):
    watcher.watch(test_application_name, 'config')
    watcher.watch(test_application_name, 'switch')
    watcher.watch(test_application_name, 'switch')
    assert len(watcher.holders) == 2


def test_ignored_events(zk, watcher, test_application_name):
    zk.ensure_path('/huskar/config/%s/stable/i/x' % test_application_name)
    watcher.watch(test_application_name, 'config')

    zk.set('/huskar/config/%s' % test_application_name, '')
    zk.set('/huskar/config/%s/stable/i/x' % test_application_name, '')
    zk.set('/huskar/config/%s/stable' % test_application_name, '')

    iterator = iter(watcher)
    assert next(iterator)[0] == 'ping'


def test_service_extras_with_route(
        mocker, route_management, instance_management, watcher, set_route,
        test_application_name):
    # Setup initial data
    set_route(test_application_name, 'alta1-channel-stable-1')
    instance, _ = instance_management.get_instance(
        'alta1-channel-stable-1', '169.254.0.1_5000', resolve=False)
    instance.data = '{}'
    instance.save()

    # Setup watcher
    watcher.with_initial = True
    watcher.from_application_name = route_management.application_name
    watcher.from_cluster_name = route_management.cluster_name
    watcher.limit_cluster_name(test_application_name, 'service', 'direct')
    watcher.watch(test_application_name, 'service')

    # Okay
    assert next(iter(watcher)) == ('all', {
        'service': {test_application_name: {'direct': {
            '169.254.0.1_5000': {
                'value': '{}',
            },
        }}},
        'switch': {},
        'config': {},
        'service_info': {},
    })

    # Setup dirty data
    instance.data = '{'
    instance.save()

    # Spin the gevent hub
    instance.load()

    # Failure
    assert next(iter(watcher)) == ('all', {
        'service': {test_application_name: {'direct': {
            '169.254.0.1_5000': {'value': '{'},
        }}},
        'switch': {},
        'config': {},
        'service_info': {},
    })


def test_detect_bad_route(
        mocker, watcher, route_management, instance_management,
        test_application_name, update_legacy_application_list,
        update_route_from_cluster_blacklist,
        update_route_dest_cluster_blacklist, set_route):
    logger = mocker.patch(
        'huskar_api.models.tree.watcher.logger', autospec=True)

    # Setup initial data
    set_route(test_application_name, 'alta1-channel-stable-1')
    instance, _ = instance_management.get_instance(
        'alta1-channel-stable-1', '169.254.0.1_5000', resolve=False)
    instance.data = '{}'
    instance.save()

    # Setup watcher
    watcher.with_initial = True
    watcher.from_application_name = route_management.application_name
    watcher.from_cluster_name = route_management.cluster_name
    watcher.limit_cluster_name(test_application_name, 'service', 'direct')
    watcher.watch(test_application_name, 'service')

    # Round 1 - The route is fine
    assert next(iter(watcher)) == ('all', {
        'service': {test_application_name: {'direct': {
            '169.254.0.1_5000': {
                'value': '{}',
            },
        }}},
        'switch': {},
        'config': {},
        'service_info': {},
    })
    assert len(logger.info.mock_calls) == 0

    # Round 2 - The route is bad
    set_route(test_application_name, 'alta1-channel-stable-2')
    assert watcher.queue.get(timeout=5) == ('all', {
        'service': {test_application_name: {'direct': {}}},
        'switch': {},
        'config': {},
        'service_info': {}
    })
    assert len(logger.info.mock_calls) == 1
    expected_cluster_map = {
        'direct': 'alta1-channel-stable-2',
    }
    logger.info.assert_called_once_with(
        'Bad route detected: %s %s %s %s -> %s (%r)',
        test_application_name, 'alta1-stable', test_application_name,
        'direct', 'alta1-channel-stable-2', expected_cluster_map)

    # Round 3 - The route is bad but this feature is switched off
    switch = mocker.patch(
        'huskar_api.models.tree.watcher.switch', autospec=True)
    switch.is_switched_on.return_value = False
    set_route(test_application_name, 'alta1-channel-stable-3')
    assert watcher.queue.get(timeout=3) == ('all', {
        'service': {test_application_name: {'direct': {}}},
        'switch': {},
        'config': {},
        'service_info': {},
    })
    assert len(logger.info.mock_calls) == 1

    # Round 4 - The route is bad but application in LEGACY_APPLICATION_LIST
    update_legacy_application_list([test_application_name])
    switch.is_switched_on.return_value = True
    set_route(test_application_name, 'alta1-channel-stable-4')
    assert watcher.queue.get() == ('all', {
        'service': {test_application_name: {'direct': {}}},
        'switch': {},
        'config': {},
        'service_info': {}
    })
    assert len(logger.info.mock_calls) == 1
    update_legacy_application_list([])

    # Round 5 - The route is bad but from_cluster_name
    # in ROUTE_FROM_CLUSTER_BLACKLIST
    update_route_from_cluster_blacklist({
        test_application_name: [watcher.from_cluster_name],
    })
    switch.is_switched_on.return_value = True
    set_route(test_application_name, 'alta1-channel-stable-5')
    assert watcher.queue.get() == ('all', {
        'service': {test_application_name: {'direct': {}}},
        'switch': {},
        'config': {},
        'service_info': {}
    })
    assert len(logger.info.mock_calls) == 1
    update_route_from_cluster_blacklist({})

    # Round 6 - The route is bad but dest_cluster_name
    # in ROUTE_DEST_CLUSTER_BLACKLIST
    update_route_dest_cluster_blacklist({
        test_application_name: ['direct'],
    })
    switch.is_switched_on.return_value = True
    set_route(test_application_name, 'alta1-channel-stable-6')
    assert watcher.queue.get() == ('all', {
        'service': {test_application_name: {'direct': {}}},
        'switch': {},
        'config': {},
        'service_info': {}
    })
    assert len(logger.info.mock_calls) == 1
    update_route_dest_cluster_blacklist({})


def test_change_service_info(zk, watcher, test_application_name):
    base_path = '/huskar/service/%s' % test_application_name
    zk.ensure_path('%s/stable' % base_path)
    watcher.watch(test_application_name, 'service_info')
    zk.set(base_path, json.dumps({'info': {'balance_policy': 'RoundRobin'}}))
    zk.set('%s/stable' % base_path, json.dumps(
        {'info': {'dict': {"port": 8080}}}))

    iterator = iter(watcher)
    assert next(iterator) == ('update', {
        'service_info': {
            test_application_name: {
                'overall': {'balance_policy': {'value': '"RoundRobin"'}}
            }
        }
    })
    assert next(iterator) == ('ping', {})
    assert next(iterator) == ('update', {
        'service_info': {
            test_application_name: {
                u'stable': {'dict': {'value': '{"port": 8080}'}}
            }
        }
    })
    assert next(iterator) == ('ping', {})


def test_change_service_info_with_limit(zk, watcher, test_application_name):
    test_application_name_bar = '{}_bar'.format(test_application_name)
    base_path = '/huskar/service/%s' % test_application_name
    zk.ensure_path('%s/stable' % base_path)
    base_path_bar = '/huskar/service/%s' % test_application_name_bar
    zk.ensure_path('%s/stable' % base_path_bar)
    watcher.watch(test_application_name, 'service_info')
    watcher.watch(test_application_name_bar, 'service_info')
    watcher.limit_cluster_name(test_application_name_bar, 'service_info', '23')

    zk.set(base_path_bar, json.dumps(
        {'info': {'balance_policy': 'RoundRobin'}}))
    zk.set('%s/stable' % base_path_bar, json.dumps(
        {'info': {'dict': {"port": 8080}}}))
    zk.set(base_path, json.dumps({'info': {'balance_policy': 'RoundRobin'}}))
    zk.set('%s/stable' % base_path, json.dumps(
        {'info': {'dict': {"port": 8080}}}))

    iterator = get_non_ping_event(iter(watcher))
    assert next(iterator) == ('update', {
        'service_info': {
            test_application_name: {
                'overall': {'balance_policy': {'value': '"RoundRobin"'}}
            }
        }
    })
    assert next(iterator) == ('update', {
        'service_info': {
            test_application_name: {
                u'stable': {'dict': {'value': '{"port": 8080}'}}
            }
        }
    })


def test_watch_multi_services(
        mocker, watcher, route_management, instance_management,
        test_application_name, set_route):
    test_application_name_bar = '{}_bar'.format(test_application_name)
    # Setup initial data
    set_route(test_application_name, 'alta1-channel-stable-1')
    set_route(test_application_name_bar, 'alta1-channel-stable-2')
    instance, _ = instance_management.get_instance(
        'alta1-channel-stable-1', '169.254.0.1_5000', resolve=False)
    instance.data = '{}'
    instance.save()

    instance_management_bar = InstanceManagement(
        huskar_client, test_application_name_bar, SERVICE_SUBDOMAIN)
    instance_bar, _ = instance_management_bar.get_instance(
        'alta1-channel-stable-2', '169.254.0.2_5000', resolve=False)
    instance_bar.data = '{}'
    instance_bar.save()

    # Setup watcher
    watcher.with_initial = True
    watcher.from_application_name = route_management.application_name
    watcher.from_cluster_name = route_management.cluster_name
    watcher.limit_cluster_name(test_application_name, 'service', 'direct')
    watcher.limit_cluster_name(test_application_name_bar, 'service', 'direct')
    watcher.holders.add(mocker.Mock(type_name='foobar'))
    watcher.watch(test_application_name, 'service')
    watcher.watch(test_application_name, 'config')
    watcher.watch(test_application_name_bar, 'service')
    watcher.watch(test_application_name_bar, 'config')

    assert next(iter(watcher)) == ('all', {
        'service': {
            test_application_name: {'direct': {
                '169.254.0.1_5000': {'value': '{}'},
            }},
            test_application_name_bar: {'direct': {
                '169.254.0.2_5000': {'value': '{}'},
            }},
        },
        'switch': {},
        'config': {test_application_name: {}, test_application_name_bar: {}},
        'service_info': {},
    })


def get_non_ping_event(iterator):
    for event in iterator:
        if event[0] == 'ping':
            continue
        yield event
