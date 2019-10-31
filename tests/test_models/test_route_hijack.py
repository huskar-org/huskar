from __future__ import absolute_import

import copy
import json

from pytest import mark, fixture

from huskar_api import settings
from huskar_api.switch import (
    switch, SWITCH_ENABLE_ROUTE_HIJACK_WITH_LOCAL_EZONE)
from huskar_api.models import huskar_client
from huskar_api.models.route.hijack import RouteHijack


@fixture()
def from_application_name(faker):
    return faker.uuid4()[:8]


@fixture()
def dest_application_name(faker):
    return faker.uuid4()[:8]


@mark.xparametrize
def test_route_hijack_mode_get_ezone_via_host(
        mocker, from_application_name, dest_application_name, zk,
        system_ezone, route_hijack_list,
        present_route_stages, default_hijack_mode,
        expected_hijack_mode, request_domain):
    mocker.patch.object(settings, 'EZONE', system_ezone)
    remote_addr = '127.0.0.1'
    from_cluster_name = ''
    ezone_cluster_map = {
        'default': 'foo-channel-stable-1',
        'overall': 'channel-stable-1',
        'fa1': 'channel-stable-1',
        'fb1': 'fb1-channel-stable-1',
        'fc1': 'fc1-channel-stable-1',
    }
    route_hijack_list = copy.deepcopy(route_hijack_list)
    present_route_stages = copy.deepcopy(present_route_stages)
    default_hijack_mode = copy.deepcopy(default_hijack_mode)
    mocker.patch.object(
        settings, 'ROUTE_EZONE_DEFAULT_HIJACK_MODE', default_hijack_mode)
    mocker.patch.object(
        settings, 'ROUTE_EZONE_CLUSTER_MAP', ezone_cluster_map)
    mocker.patch.object(
        settings, 'ROUTE_DOMAIN_EZONE_MAP', {
            'a.example.com': 'fa1',
            'b.example.com': 'fb1',
            'c.example.com': 'fc1',
            '0.example.com': 'fd1',
        })

    if '<application_name>' in route_hijack_list:
        route_hijack_list[from_application_name] = route_hijack_list.pop(
            '<application_name>')
    mocker.patch.object(
        settings, 'ROUTE_HIJACK_LIST', route_hijack_list)

    zk.delete('/huskar/config/arch.huskar_api', recursive=True)
    for cluster_name, route_stage_table in present_route_stages.items():
        if '<application_name>' in route_stage_table:
            route_stage_table[from_application_name] = route_stage_table.pop(
                '<application_name>')
        zk.create('/huskar/config/arch.huskar_api/{}'.format(cluster_name),
                  makepath=True)
        path = '/huskar/config/arch.huskar_api/{0}/ROUTE_HIJACK_LIST'.format(
            cluster_name)
        zk.create(path, json.dumps(route_stage_table))

    route_hijack = RouteHijack(
        huskar_client, from_application_name, from_cluster_name,
        remote_addr, 'orig', request_domain)

    assert route_hijack.hijack_mode.value == expected_hijack_mode


@mark.xparametrize
def test_route_hijack_mode_get_ezone_via_cluster(
        mocker, from_application_name, dest_application_name, zk,
        ezone_list, system_ezone, from_cluster_name,
        route_hijack_list, present_route_stages, default_hijack_mode,
        expected_hijack_mode):

    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_HIJACK_WITH_LOCAL_EZONE:
            return True
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    mocker.patch.object(settings, 'EZONE', system_ezone)
    remote_addr = '127.0.0.1'
    request_domain = '127.0.0.1'
    ezone_cluster_map = {
        'default': 'foo-channel-stable-1',
        'overall': 'channel-stable-1',
        'fa1': 'channel-stable-1',
        'fb1': 'fb1-channel-stable-1',
        'fc1': 'fc1-channel-stable-1',
    }
    ezone_list = copy.deepcopy(ezone_list)
    route_hijack_list = copy.deepcopy(route_hijack_list)
    present_route_stages = copy.deepcopy(present_route_stages)
    default_hijack_mode = copy.deepcopy(default_hijack_mode)
    mocker.patch.object(
        settings, 'ROUTE_EZONE_DEFAULT_HIJACK_MODE', default_hijack_mode)
    mocker.patch.object(
        settings, 'ROUTE_EZONE_LIST', ezone_list)
    mocker.patch.object(
        settings, 'ROUTE_EZONE_CLUSTER_MAP', ezone_cluster_map)
    mocker.patch.object(settings, 'ROUTE_OVERALL_EZONE', 'fa1')

    if '<application_name>' in route_hijack_list:
        route_hijack_list[from_application_name] = route_hijack_list.pop(
            '<application_name>')
    mocker.patch.object(
        settings, 'ROUTE_HIJACK_LIST', route_hijack_list)

    zk.delete('/huskar/config/arch.huskar_api', recursive=True)
    for cluster_name, route_stage_table in present_route_stages.items():
        if '<application_name>' in route_stage_table:
            route_stage_table[from_application_name] = route_stage_table.pop(
                '<application_name>')
        zk.create('/huskar/config/arch.huskar_api/{}'.format(cluster_name),
                  makepath=True)
        path = '/huskar/config/arch.huskar_api/{0}/ROUTE_HIJACK_LIST'.format(
            cluster_name)
        zk.create(path, json.dumps(route_stage_table))

    route_hijack = RouteHijack(
        huskar_client, from_application_name, from_cluster_name,
        remote_addr, 'orig', request_domain)

    assert route_hijack.hijack_mode.value == expected_hijack_mode
