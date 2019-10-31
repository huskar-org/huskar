from __future__ import absolute_import

import datetime

from pytest import raises

from huskar_api.models.audit.index import AuditIndex, AuditIndexInstance
from huskar_api.models.audit.const import TYPE_SITE, TYPE_TEAM, TYPE_CONFIG


def test_audit_index(db):
    # The AuditIndex is an internal model. So we test it simply.
    date = datetime.date.today()
    assert AuditIndex.get_audit_ids(TYPE_SITE, 0) == []
    assert AuditIndex.get_audit_ids(TYPE_TEAM, 1) == []
    assert AuditIndex.get_audit_ids(TYPE_TEAM, 3) == []
    assert AuditIndex.get_audit_ids_by_date(TYPE_SITE, 0, date) == []
    assert AuditIndex.get_audit_ids_by_date(TYPE_TEAM, 1, date) == []
    assert AuditIndex.get_audit_ids_by_date(TYPE_TEAM, 3, date) == []

    created_at = datetime.datetime.now()
    date = created_at.date()
    with db.close_on_exit(False):
        AuditIndex.create(db, 1, created_at, TYPE_SITE, 0)
        AuditIndex.create(db, 1, created_at, TYPE_TEAM, 1)
        AuditIndex.create(db, 2, created_at, TYPE_TEAM, 1)
        with raises(AssertionError):
            AuditIndex.create(db, 2, created_at, TYPE_SITE, 1)
    AuditIndex.flush_cache(date, TYPE_SITE, 0)
    AuditIndex.flush_cache(date, TYPE_TEAM, 1)
    AuditIndex.flush_cache(date, TYPE_TEAM, 1)

    assert AuditIndex.get_audit_ids(TYPE_SITE, 0) == [1]
    assert AuditIndex.get_audit_ids(TYPE_TEAM, 1) == [2, 1]
    assert AuditIndex.get_audit_ids(TYPE_TEAM, 3) == []
    assert AuditIndex.get_audit_ids_by_date(TYPE_SITE, 0, date) == [1]
    assert AuditIndex.get_audit_ids_by_date(TYPE_TEAM, 1, date) == [2, 1]
    assert AuditIndex.get_audit_ids_by_date(TYPE_TEAM, 3, date) == []


def test_audit_instance_index(db):
    application_id = 1
    cluster_name = 'bar'
    key = 'test'
    now = datetime.datetime.now()

    assert AuditIndexInstance.get_audit_ids(
        TYPE_CONFIG, application_id, cluster_name, key) == []
    with db.close_on_exit(False):
        AuditIndexInstance.create(
            db, 1, now, TYPE_CONFIG, application_id, cluster_name, key)
        with raises(AssertionError):
            AuditIndexInstance.create(
                db, 1, now, application_id, cluster_name, key, -1)

    AuditIndexInstance.flush_cache(
        now.date(), TYPE_CONFIG, application_id, cluster_name, key)
    assert AuditIndexInstance.get_audit_ids(
        TYPE_CONFIG, application_id, cluster_name, key) == [1]
