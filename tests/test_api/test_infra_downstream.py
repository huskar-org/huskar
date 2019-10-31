from __future__ import absolute_import

from huskar_api.models.infra import InfraDownstream
from ..utils import assert_response_ok


def test_get_infra_downstream(client, test_token):
    InfraDownstream.bindmany() \
        .bind('base.foo', 'redis', 'cache-1', 'idcs', 'alta1', 'url',
              'redis.100010') \
        .bind('base.foo', 'redis', 'cache-1', 'idcs', 'altb1', 'url',
              'redis.100010') \
        .bind('base.bar', 'redis', 'cache-1', 'idcs', 'alta1', 'url',
              'redis.100010') \
        .bind('base.bar', 'redis', 'cache-2', 'idcs', 'alta1', 'url',
              'redis.100011') \
        .commit()

    r = client.get('/api/infra-config-downstream/redis.100010', headers={
        'Authorization': test_token})
    assert_response_ok(r)
    downstream = r.json['data']['downstream']
    assert len(downstream) == 3
    assert downstream[0]['user_application_name'] == 'base.foo'
    assert downstream[0]['user_infra_type'] == 'redis'
    assert downstream[0]['user_infra_name'] == 'cache-1'
    assert downstream[0]['user_scope_pair'] == {
        'type': 'idcs', 'name': 'alta1'}
    assert downstream[0]['user_field_name'] == 'url'
    assert downstream[1]['user_application_name'] == 'base.foo'
    assert downstream[1]['user_infra_type'] == 'redis'
    assert downstream[1]['user_infra_name'] == 'cache-1'
    assert downstream[1]['user_scope_pair'] == {
        'type': 'idcs', 'name': 'altb1'}
    assert downstream[1]['user_field_name'] == 'url'
    assert downstream[2]['user_application_name'] == 'base.bar'
    assert downstream[2]['user_infra_type'] == 'redis'
    assert downstream[2]['user_infra_name'] == 'cache-1'
    assert downstream[2]['user_scope_pair'] == {
        'type': 'idcs', 'name': 'alta1'}
    assert downstream[2]['user_field_name'] == 'url'

    r = client.get('/api/infra-config-downstream/redis.100011', headers={
        'Authorization': test_token})
    assert_response_ok(r)
    downstream = r.json['data']['downstream']
    assert len(downstream) == 1
    assert downstream[0]['user_application_name'] == 'base.bar'
    assert downstream[0]['user_infra_type'] == 'redis'
    assert downstream[0]['user_infra_name'] == 'cache-2'
    assert downstream[0]['user_scope_pair'] == {
        'type': 'idcs', 'name': 'alta1'}
    assert downstream[0]['user_field_name'] == 'url'

    InfraDownstream.bind(
        'base.baz', 'redis', 'cache-1', 'idcs', 'alta1', 'url', 'redis.100011')
    InfraDownstream.unbind(
        'base.bar', 'redis', 'cache-2', 'idcs', 'alta1', 'url')

    # Stale data
    r = client.get('/api/infra-config-downstream/redis.100011', headers={
        'Authorization': test_token})
    assert_response_ok(r)
    downstream = r.json['data']['downstream']
    assert len(downstream) == 1
    assert downstream[0]['user_application_name'] == 'base.bar'
    assert downstream[0]['user_infra_name'] == 'cache-2'
    assert downstream[0]['user_scope_pair'] == {
        'type': 'idcs', 'name': 'alta1'}
    assert downstream[0]['user_field_name'] == 'url'

    # Fresh data
    r = client.post('/api/infra-config-downstream/redis.100011', headers={
        'Authorization': test_token})
    assert_response_ok(r)
    downstream = r.json['data']['downstream']
    assert len(downstream) == 1
    assert downstream[0]['user_application_name'] == 'base.baz'
    assert downstream[0]['user_infra_name'] == 'cache-1'
    assert downstream[0]['user_scope_pair'] == {
        'type': 'idcs', 'name': 'alta1'}
    assert downstream[0]['user_field_name'] == 'url'
