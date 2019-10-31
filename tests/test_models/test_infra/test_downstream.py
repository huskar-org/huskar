# coding: utf-8
from __future__ import absolute_import

from huskar_api.models.infra import InfraDownstream


def list_infra_downstream_by_application_name(db, application_name):
    db.close()  # Clear identity map
    return db.query(InfraDownstream) \
        .filter_by(application_name=application_name) \
        .order_by(InfraDownstream.id.asc()) \
        .all()


def test_bind(db):
    InfraDownstream.bind(
        'base.foo', 'redis', 'mycache', 'idcs', 'alta1', 'url', 'redis.foo')
    InfraDownstream.bind(
        'base.bar', 'redis', 'mycache', 'idcs', 'alta1', 'url', 'redis.foo')
    InfraDownstream.bind(
        'base.bar', 'redis', 'mycache', 'idcs', 'altb1', 'url', 'redis.foo')
    InfraDownstream.bind(
        'base.bar', 'redis', u'缓存', 'idcs', 'alta1', 'url', 'redis.foo')
    result = list_infra_downstream_by_application_name(db, 'redis.foo')
    assert len(result) == 4
    assert result[0].user_application_name == 'base.foo'
    assert result[0].user_scope_name == 'alta1'
    assert result[1].user_application_name == 'base.bar'
    assert result[1].user_scope_name == 'alta1'
    assert result[2].user_application_name == 'base.bar'
    assert result[2].user_scope_name == 'altb1'
    assert result[3].user_application_name == 'base.bar'
    assert result[3].user_scope_name == 'alta1'
    assert result[3].user_infra_name == u'缓存'
    result = list_infra_downstream_by_application_name(db, 'redis.bar')
    assert len(result) == 0

    InfraDownstream.bind(
        'base.bar', 'redis', 'mycache', 'idcs', 'alta1', 'url', 'redis.bar')
    result = list_infra_downstream_by_application_name(db, 'redis.foo')
    assert len(result) == 3
    assert result[0].user_application_name == 'base.foo'
    assert result[0].user_scope_name == 'alta1'
    assert result[1].user_application_name == 'base.bar'
    assert result[1].user_scope_name == 'altb1'
    result = list_infra_downstream_by_application_name(db, 'redis.bar')
    assert len(result) == 1
    assert result[0].user_application_name == 'base.bar'
    assert result[0].user_scope_name == 'alta1'


def test_bindmany(db):
    m = InfraDownstream.bindmany() \
        .bind('base.foo', 'redis', 'mycache', 'idcs', 'alta1', 'url',
              'redis.foo') \
        .bind('base.bar', 'redis', 'mycache', 'idcs', 'alta1', 'url',
              'redis.foo') \
        .commit()
    result = list_infra_downstream_by_application_name(db, 'redis.foo')
    assert len(result) == 2
    assert result[0].user_application_name == 'base.foo'
    assert result[1].user_application_name == 'base.bar'
    result = list_infra_downstream_by_application_name(db, 'redis.bar')
    assert len(result) == 0

    m.bindmany() \
        .bind('base.bar', 'redis', 'mycache', 'idcs', 'alta1', 'url',
              'redis.bar') \
        .commit()
    result = list_infra_downstream_by_application_name(db, 'redis.foo')
    assert len(result) == 1
    assert result[0].user_application_name == 'base.foo'
    result = list_infra_downstream_by_application_name(db, 'redis.bar')
    assert len(result) == 1
    assert result[0].user_application_name == 'base.bar'


def test_unbind_stale(db):
    m1 = InfraDownstream.bindmany() \
        .bind('base.foo', 'redis', 'mycache', 'idcs', 'alta1', 'url',
              'redis.foo') \
        .bind('base.bar', 'redis', 'mycache', 'idcs', 'alta1', 'url',
              'redis.foo') \
        .commit()
    result = list_infra_downstream_by_application_name(db, 'redis.foo')
    assert len(result) == 2

    m2 = InfraDownstream.bindmany() \
        .bind('base.foo', 'redis', 'mycache', 'idcs', 'alta1', 'url',
              'redis.foo') \
        .unbind_stale() \
        .commit()
    result = list_infra_downstream_by_application_name(db, 'redis.foo')
    assert len(result) == 1
    assert result[0].user_application_name == 'base.foo'
    assert result[0].version == m2._timestamp
    assert result[0].version > m1._timestamp


def test_unbind(db):
    InfraDownstream.bind(
        'base.foo', 'redis', 'mycache', 'idcs', 'alta1', 'url', 'redis.foo')
    InfraDownstream.bind(
        'base.foo', 'redis', 'mycache', 'idcs', 'altb1', 'url', 'redis.foo')
    InfraDownstream.bind(
        'base.bar', 'redis', 'mycache', 'idcs', 'alta1', 'url', 'redis.foo')
    result = list_infra_downstream_by_application_name(db, 'redis.foo')
    assert len(result) == 3

    InfraDownstream.unbind(
        'base.bar', 'redis', 'mycache', 'idcs', 'alta1', 'url')
    result = list_infra_downstream_by_application_name(db, 'redis.foo')
    assert len(result) == 2
    assert result[0].user_application_name == 'base.foo'
    assert result[0].user_scope_name == 'alta1'
    assert result[1].user_application_name == 'base.foo'
    assert result[1].user_scope_name == 'altb1'


def test_get_multi_by_application(db):
    InfraDownstream.bindmany() \
        .bind('base.foo', 'redis', 'mycache', 'idcs', 'alta1', 'url',
              'redis.foo') \
        .bind('base.bar', 'redis', 'mycache', 'idcs', 'alta1', 'url',
              'redis.foo') \
        .bind('base.bar', 'redis', 'frog+1s', 'idcs', 'alta1', 'url',
              'redis.bar') \
        .bind('base.bar', 'redis', 'frog+1s', 'idcs', 'altb1', 'url',
              'redis.bar') \
        .commit()

    r = InfraDownstream.get_multi_by_application('redis.foo')
    assert len(r) == 2
    assert r[0].user_application_name == 'base.foo'
    assert r[0].user_infra_type == 'redis'
    assert r[0].user_infra_name == 'mycache'
    assert r[0].user_scope_name == 'alta1'
    assert r[1].user_application_name == 'base.bar'
    assert r[1].user_infra_type == 'redis'
    assert r[1].user_infra_name == 'mycache'
    assert r[1].user_scope_name == 'alta1'

    r = InfraDownstream.get_multi_by_application('redis.bar')
    assert len(r) == 2
    assert r[0].user_application_name == 'base.bar'
    assert r[0].user_infra_type == 'redis'
    assert r[0].user_infra_name == 'frog+1s'
    assert r[0].user_scope_name == 'alta1'
    assert r[1].user_application_name == 'base.bar'
    assert r[1].user_infra_type == 'redis'
    assert r[1].user_infra_name == 'frog+1s'
    assert r[1].user_scope_name == 'altb1'


def test_bind_various_scopes_and_fields(db):
    InfraDownstream.bindmany() \
        .bind('base.foo', 'database', 'mydb', 'idcs', 'alta1', 'master',
              'dal.test.master') \
        .bind('base.foo', 'database', 'mydb', 'idcs', 'alta1', 'slave',
              'dal.test.auto') \
        .bind('base.foo', 'database', 'mydb', 'clusters', 'sandbox', 'master',
              'dal.sandbox.master') \
        .bind('base.foo', 'database', 'mydb', 'clusters', 'sandbox', 'slave',
              'dal.sandbox.auto') \
        .bind('base.foo', 'database', 'mydb', 'idcs', 'altb1', 'master',
              'dal.test.master') \
        .bind('base.foo', 'database', 'mydb', 'idcs', 'altb1', 'slave',
              'dal.test.auto') \
        .commit()

    r = InfraDownstream.get_multi_by_application('dal.test.master')
    assert len(r) == 2
    assert r[0].user_application_name == 'base.foo'
    assert r[0].user_infra_type == 'database'
    assert r[0].user_infra_name == 'mydb'
    assert r[0].user_scope_pair == ('idcs', 'alta1')
    assert r[0].user_field_name == 'master'
    assert r[1].user_application_name == 'base.foo'
    assert r[1].user_infra_type == 'database'
    assert r[1].user_infra_name == 'mydb'
    assert r[1].user_scope_pair == ('idcs', 'altb1')
    assert r[1].user_field_name == 'master'

    r = InfraDownstream.get_multi_by_application('dal.test.auto')
    assert len(r) == 2
    assert r[0].user_application_name == 'base.foo'
    assert r[0].user_infra_type == 'database'
    assert r[0].user_infra_name == 'mydb'
    assert r[0].user_scope_pair == ('idcs', 'alta1')
    assert r[0].user_field_name == 'slave'
    assert r[1].user_application_name == 'base.foo'
    assert r[1].user_infra_type == 'database'
    assert r[1].user_infra_name == 'mydb'
    assert r[1].user_scope_pair == ('idcs', 'altb1')
    assert r[1].user_field_name == 'slave'

    r = InfraDownstream.get_multi_by_application('dal.sandbox.master')
    assert len(r) == 1
    assert r[0].user_application_name == 'base.foo'
    assert r[0].user_infra_type == 'database'
    assert r[0].user_infra_name == 'mydb'
    assert r[0].user_scope_pair == ('clusters', 'sandbox')
    assert r[0].user_field_name == 'master'

    r = InfraDownstream.get_multi_by_application('dal.sandbox.auto')
    assert len(r) == 1
    assert r[0].user_application_name == 'base.foo'
    assert r[0].user_infra_type == 'database'
    assert r[0].user_infra_name == 'mydb'
    assert r[0].user_scope_pair == ('clusters', 'sandbox')
    assert r[0].user_field_name == 'slave'
