from __future__ import absolute_import

import json

from huskar_sdk_v2.consts import BASE_PATH, SERVICE_SUBDOMAIN
from huskar_sdk_v2.utils import combine
from kazoo.exceptions import NoNodeError
from pytest import fixture, mark, raises

from huskar_api.models import huskar_client
from huskar_api.models.audit import action_types, action_creator, AuditLog
from huskar_api.models.instance import InstanceManagement, InfraInfo
from huskar_api.models.route import RouteManagement
from ..utils import assert_response_ok


@fixture
def test_application_name(test_application):
    return test_application.application_name


@fixture
def dest_application_name(faker):
    return faker.uuid4()[:8]


@fixture
def zk_path(test_application_name):
    def wrapped(data_type, key):
        cluster_name = 'test'
        return combine(BASE_PATH, data_type, test_application_name,
                       cluster_name, key)
    return wrapped


@fixture
def instance_management(test_application_name):
    return InstanceManagement(
        huskar_client, test_application_name, SERVICE_SUBDOMAIN)


@fixture
def add_instance(zk, zk_path):
    def wrapped(action_type, key, value):
        _, data_type = action_types[action_type].split('_', 1)
        path = zk_path(data_type.lower(), key)
        zk.create(path, value, makepath=True)
    return wrapped


@fixture
def make_instance_auditlog(zk, db, zk_path, faker):

    def wrapped(action_type, key, new_data, old_data):
        _, data_type = action_types[action_type].split('_', 1)
        path = zk_path(data_type.lower(), key)
        path_fragment = path.split('/')
        application_name, cluster_name = path_fragment[3:5]

        action = action_creator.make_action(
            action_type, application_name=application_name,
            cluster_name=cluster_name, key=key, old_data=old_data,
            new_data=new_data)
        audit_log = AuditLog.create(0, faker.ipv4(), action)
        db.close()
        return audit_log
    return wrapped


@fixture
def make_cluster_link_auditlog(db, test_application_name, faker):

    def wrapped(action_type, cluster, link=None):
        action = action_creator.make_action(
            action_type,
            application_name=test_application_name,
            cluster_name=cluster,
            physical_name=link
        )
        audit_log = AuditLog.create(0, faker.ipv4(), action)
        db.close()
        return audit_log


@mark.parametrize('action_type,new_data,old_data', [
    (action_types.UPDATE_SERVICE, 'foo', 'bar'),
    (action_types.UPDATE_SERVICE, 'foo', None),
    (action_types.UPDATE_SWITCH, 'foo', 'bar'),
    (action_types.UPDATE_CONFIG, 'foo', 'bar'),
    (action_types.DELETE_SERVICE, None, 'foo'),
    (action_types.DELETE_CONFIG, None, 'foo'),
    (action_types.DELETE_SWITCH, None, 'foo'),
])
def test_rollback_instance_configuration(
        zk, client, faker, admin_token, test_application_name, last_audit_log,
        add_instance, make_instance_auditlog, zk_path, action_type,
        new_data, old_data):
    key = 'foo'
    now_value = faker.uuid4()[:8]
    method_type, data_type = action_types[action_type].split('_', 1)
    path = zk_path(data_type.lower(), key)
    audit_log = make_instance_auditlog(action_type, key, new_data, old_data)
    add_instance(action_type, key, now_value)
    value, _ = zk.get(path)
    assert value == now_value

    r = client.put(
        '/api/audit-rollback/%s/%s' % (test_application_name, audit_log.id),
        headers={'Authorization': admin_token})
    assert_response_ok(r)

    if old_data is None:
        with raises(NoNodeError):
            value, _ = zk.get(path)
    else:
        value, _ = zk.get(path)
        assert value == old_data
    last_audit = last_audit_log()
    assert last_audit.id != audit_log.id
    action_data = json.loads(last_audit.action_data)
    assert action_data['data'] == {'new': old_data, 'old': now_value}


@mark.parametrize('action_type,cluster,link_to,final_link_to', [
    (action_types.ASSIGN_CLUSTER_LINK, 'test', 'alpha_stable', None),
    (action_types.DELETE_CLUSTER_LINK, 'test', 'alpha_stable', 'alpha_stable'),
])
def test_rollback_cluster_link_change(
        client, faker, admin_token, db, action_type, test_application_name,
        instance_management, cluster, link_to, final_link_to):
    action = action_creator.make_action(
        action_type,
        application_name=test_application_name,
        cluster_name=cluster,
        physical_name=link_to,
    )
    audit_log = AuditLog.create(0, faker.ipv4(), action)
    db.close()

    if action_type == action_types.ASSIGN_CLUSTER_LINK:
        cluster_info = instance_management.get_cluster_info(cluster)
        cluster_info.set_link(link_to)
        cluster_info.save()
    else:
        instance, _ = instance_management.get_instance(
            link_to, 'key', resolve=False)
        instance.data = 'value'
        instance.save()

    r = client.put(
        '/api/audit-rollback/%s/%s' % (test_application_name, audit_log.id),
        headers={'Authorization': admin_token})
    assert_response_ok(r)

    cluster_info = instance_management.get_cluster_info(cluster)
    assert cluster_info.get_link() == final_link_to


@mark.parametrize('action_type', [
    action_types.ASSIGN_CLUSTER_LINK, action_types.DELETE_CLUSTER_LINK])
def test_rollback_cluster_link_conflict(
        client, faker, db, test_application_name,
        admin_token, instance_management, action_type):
    action = action_creator.make_action(
        action_type,
        application_name=test_application_name,
        cluster_name='test',
        physical_name='alpha_stable',
    )
    audit_log = AuditLog.create(0, faker.ipv4(), action)
    db.close()

    instance, _ = instance_management.get_instance(
        'diff', 'key', resolve=False)
    instance.data = 'value'
    instance.save()

    cluster_info = instance_management.get_cluster_info('test')
    cluster_info.set_link('diff')
    cluster_info.save()

    r = client.put(
        '/api/audit-rollback/%s/%s' % (test_application_name, audit_log.id),
        headers={'Authorization': admin_token})
    assert r.status_code == 409


@mark.parametrize(
    'action_type,cluster_name,destination_cluster_name', [
        (action_types.UPDATE_ROUTE, 'foo', 'bar'),
        (action_types.DELETE_ROUTE, 'foo', 'bar')
    ]
)
def test_rollback_route_change(
        db, client, faker, test_application_name, admin_token, action_type,
        cluster_name, dest_application_name, destination_cluster_name, zk):
    action = action_creator.make_action(
        action_type,
        application_name=test_application_name,
        cluster_name=cluster_name,
        intent='direct',
        dest_application_name=dest_application_name,
        dest_cluster_name=destination_cluster_name
    )
    audit_log = AuditLog.create(0, faker.ipv4(), action)
    db.close()

    rm = RouteManagement(huskar_client, test_application_name, cluster_name)
    prev_destination_cluster = faker.uuid4()[:8]
    for cluster in [destination_cluster_name, prev_destination_cluster]:
        path = '/huskar/service/%s/%s/fo' % (dest_application_name, cluster)
        zk.ensure_path(path)
    rm.set_route(dest_application_name, prev_destination_cluster)

    r = client.put(
        '/api/audit-rollback/%s/%s' % (test_application_name, audit_log.id),
        headers={'Authorization': admin_token}
    )
    assert_response_ok(r)
    if action_type == action_types.DELETE_ROUTE:
        result = [(dest_application_name, 'direct', destination_cluster_name)]
    else:
        result = []
    assert list(rm.list_route()) == result


def test_rollback_failed(client, db, faker, test_application_name,
                         admin_token):
    fake_audit_id = int(faker.numerify())
    action = action_creator.make_action(
        action_types.CREATE_CONFIG_CLUSTER,
        application_name=test_application_name,
        cluster_name='bar'
    )
    audit_log = AuditLog.create(0, faker.ipv4(), action)
    db.close()

    r = client.put(
        '/api/audit-rollback/%s/%s' % (test_application_name, fake_audit_id),
        headers={'Authorization': admin_token}
    )
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'The audit log not existed.'

    r = client.put(
        '/api/audit-rollback/%s/%s' % (test_application_name, audit_log.id),
        headers={'Authorization': admin_token}
    )
    assert r.status_code == 400
    assert r.json['message'] == 'The audit log can\'t be rollbacked.'


@mark.xparametrize
def test_rollback_infra_config_change(
        client, db, faker, test_application_name, admin_token, last_audit_log,
        _action_type, _scope_type, _scope_name, _old_value, _new_value,
        _expected_action_type, _expected_value):
    infra_type = 'redis'
    infra_name = 'default'
    infra_info = InfraInfo(
        huskar_client.client, test_application_name, infra_type)
    infra_info.load()

    action_type = getattr(action_types, _action_type)
    action = action_creator.make_action(
        action_type,
        application_name=test_application_name,
        infra_type=infra_type,
        infra_name=infra_name,
        scope_type=_scope_type,
        scope_name=_scope_name,
        old_value=_old_value,
        new_value=_new_value,
    )
    audit_log = AuditLog.create(0, faker.ipv4(), action)
    db.close()

    r = client.put(
        '/api/audit-rollback/%s/%s' % (test_application_name, audit_log.id),
        headers={'Authorization': admin_token}
    )
    assert_response_ok(r)

    last_audit = last_audit_log()
    infra_info.load()
    value = infra_info.get_by_name(infra_name, _scope_type, _scope_name)
    assert value == _expected_value
    assert last_audit.action_type == getattr(
        action_types, _expected_action_type)
