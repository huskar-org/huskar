from __future__ import absolute_import

from huskar_api import settings
from ..utils import assert_response_ok


def test_wellknown_common(mocker, client):
    framework_versions = {
        'latest': {
            'test_1': '233',
            'test_2': '666',
        }
    }
    route_ezone_default_hijack_mode = {
        'alta1': 'S',
        'altb1': 'D',
        'altc1': 'D',
    }
    idc_list = ['adca', 'alta', 'altb']
    ezone_list = ['alta1', 'altb1', 'altc1']
    force_routing_clusters = {"alta-test@direct": "alta-test"}
    mocker.patch.object(settings, 'FRAMEWORK_VERSIONS', framework_versions)
    mocker.patch.object(settings, 'ROUTE_IDC_LIST', idc_list)
    mocker.patch.object(settings, 'ROUTE_EZONE_LIST', ezone_list)
    mocker.patch.object(settings, 'ROUTE_EZONE_DEFAULT_HIJACK_MODE',
                        route_ezone_default_hijack_mode)
    mocker.patch.object(settings, 'FORCE_ROUTING_CLUSTERS',
                        force_routing_clusters)

    r = client.get('/api/.well-known/common')
    assert_response_ok(r)
    data = r.json['data']
    assert data == {
        'framework_versions': framework_versions,
        'idc_list': idc_list,
        'ezone_list': ezone_list,
        'route_default_hijack_mode': route_ezone_default_hijack_mode,
        'force_routing_clusters': force_routing_clusters,
    }


def test_update_framework_versions():
    framework_versions = {
        'latest': {
            'test_1': '233',
            'test_2': '666',
        }
    }
    try:
        assert settings.FRAMEWORK_VERSIONS == {}
        settings.update_framework_versions(framework_versions)
        assert settings.FRAMEWORK_VERSIONS == framework_versions
    finally:
        settings.update_framework_versions({})
