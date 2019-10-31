from __future__ import absolute_import

from pytest import fixture

from huskar_api.models.auth import Application, Team
from huskar_api.models.infra import InfraDownstream
from huskar_api.models.instance import InfraInfo
from huskar_api.scripts.infra import (
    collect_infra_config, _collect_infra_upstream)


@fixture(scope='function')
def test_team(db, faker):
    team_name = 'test_%s' % faker.uuid4()[:8]
    return Team.create(team_name)


def test_collect_infra_config(zk, db, faker, test_team):
    prefix = faker.uuid4()[:8]
    for x in 'foo', 'bar':
        x = '%s_%s' % (prefix, x)
        Application.create(x, test_team.id)
        InfraDownstream.bindmany()\
            .bind(x, 'database', 'stale-db', 'idcs', 'altb1', 'master',
                  'dal.foo.master') \
            .bind(x, 'database', 'stale-db', 'idcs', 'altb1', 'slave',
                  'dal.foo.auto') \
            .bind(x, 'redis', 'stale-cache', 'idcs', 'altb1', 'url',
                  'redis.foo') \
            .commit()
        mysql_url = {
            'master': 'sam+mysql://root:root@dal.foo.master/%s_db' % x,
            'slave': 'sam+mysql://root:root@dal.foo.auto/%s_db' % x}
        redis_url = {'url': 'sam+redis://redis.foo'}
        infra_info = InfraInfo(zk, x, 'database')
        infra_info.load()
        infra_info.set_by_name('db', 'idcs', 'altb1', mysql_url)
        infra_info.save()
        infra_info = InfraInfo(zk, x, 'redis')
        infra_info.load()
        infra_info.set_by_name('cache', 'idcs', 'altb1', redis_url)
        infra_info.save()

    InfraDownstream.flush_cache_by_application('dal.foo.auto')
    ds = InfraDownstream.get_multi_by_application('dal.foo.auto')
    assert all(d.user_infra_name == 'stale-db' for d in ds)

    InfraDownstream.flush_cache_by_application('redis.auto')
    ds = InfraDownstream.get_multi_by_application('redis.foo')
    assert all(d.user_infra_name == 'stale-cache' for d in ds)

    list(_collect_infra_upstream(zk, u'error\u00a0'))

    collect_infra_config()

    InfraDownstream.flush_cache_by_application('dal.foo.auto')
    ds = InfraDownstream.get_multi_by_application('dal.foo.auto')
    assert len(ds) == 2
    assert ds[0].user_application_name == '%s_foo' % prefix
    assert ds[0].user_infra_type == 'database'
    assert ds[0].user_infra_name == 'db'
    assert ds[0].user_scope_pair == ('idcs', 'altb1')
    assert ds[0].user_field_name == 'slave'
    assert ds[1].user_application_name == '%s_bar' % prefix
    assert ds[1].user_infra_type == 'database'
    assert ds[1].user_infra_name == 'db'
    assert ds[1].user_scope_pair == ('idcs', 'altb1')
    assert ds[1].user_field_name == 'slave'

    InfraDownstream.flush_cache_by_application('redis.auto')
    ds = InfraDownstream.get_multi_by_application('redis.foo')
    assert len(ds) == 2
    assert ds[0].user_application_name == '%s_foo' % prefix
    assert ds[0].user_infra_type == 'redis'
    assert ds[0].user_infra_name == 'cache'
    assert ds[0].user_scope_pair == ('idcs', 'altb1')
    assert ds[0].user_field_name == 'url'
    assert ds[1].user_application_name == '%s_bar' % prefix
    assert ds[1].user_infra_type == 'redis'
    assert ds[1].user_infra_name == 'cache'
    assert ds[1].user_scope_pair == ('idcs', 'altb1')
    assert ds[1].user_field_name == 'url'
