from __future__ import absolute_import

import json

import gevent
from pytest import fixture, mark, raises
from gevent import spawn
from gevent.queue import Queue, Empty

from huskar_api import settings
from huskar_api.models import huskar_client
from huskar_api.models.exceptions import TreeTimeoutError
from huskar_api.models.auth import Application, User, Authority
from huskar_api.models.route import RouteManagement
from huskar_api.models.catalog import ServiceInfo
from huskar_api.models.tree.holder import TreeHolder
from huskar_api.switch import (
    switch, SWITCH_ENABLE_ROUTE_HIJACK,
    SWITCH_ENABLE_LONG_POLLING_MAX_LIFE_SPAN,
    SWITCH_ENABLE_DECLARE_UPSTREAM, SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS)


@fixture
def test_application_name(test_application):
    return test_application.application_name


@fixture(scope='function')
def test_dest_application(db, faker, test_team, test_application):
    application_name = faker.uuid4()[:8]
    application = Application.create(application_name, test_team.id)
    user = User.get_by_name(application_name)
    test_application.ensure_auth(Authority.WRITE, user.id)
    application.ensure_auth(Authority.WRITE, User.get_by_name(
        test_application.application_name).id)
    return application


@fixture
def dest_application_name(test_dest_application):
    return test_dest_application.application_name


@fixture(scope='function')
def long_poll(request, client, test_application_name, test_application_token):
    def make_long_poll(
            config=[], switch=['stable'], service=['stable', 'foo'],
            service_info=None, life_span=None, use_route=False,
            custom_payload=None, current_cluster_name=None):
        queue = Queue()

        def producer():
            url = '/api/data/long_poll'
            payload = custom_payload or {
                'config': {test_application_name: list(config)},
                'switch': {test_application_name: list(switch)},
                'service': {test_application_name: list(service)},
            }
            if service_info is not None:
                payload.update({
                    'service_info': {test_application_name: list(service_info)}
                })
            data = json.dumps(payload)
            headers = {'Authorization': test_application_token}
            if use_route:
                headers['X-SOA-Mode'] = 'route'
                headers['X-Cluster-Name'] = current_cluster_name or 'stable'
            else:
                headers['X-SOA-Mode'] = 'prefix'
                if current_cluster_name:
                    headers['X-Cluster-Name'] = current_cluster_name
            r = client.post(
                url, content_type='application/json', data=data,
                query_string={'life_span': life_span or 0}, headers=headers)
            try:
                assert r.status_code == 200, r.data

                for line in r.response:
                    item = json.loads(line.strip())
                    if item['message'] != 'ping':
                        queue.put(item)
            finally:
                r.close()

        t = spawn(producer)

        def finalizer():
            try:
                t.get(block=False)
            except gevent.Timeout:
                t.kill()
                t.get(block=False)

        request.addfinalizer(finalizer)
        queue.t = t  # for debugging
        return queue
    return make_long_poll


def test_route_force_cluster_hot_config():
    def notify(value):
        watchers = settings.config_manager.external_watchers
        for watcher in watchers['FORCE_ROUTING_CLUSTERS']:
            watcher(value)
    try:
        notify({'test-pre': 'test-pre'})
        assert settings.FORCE_ROUTING_CLUSTERS == {'test-pre': 'test-pre'}

        notify(None)
        assert settings.FORCE_ROUTING_CLUSTERS == {}
    finally:
        notify(None)


def j(ip, port, cluster, **kwargs):
    port = {'main': port}
    value = {'ip': ip, 'port': port}
    value.update(kwargs)
    return json.dumps(value, sort_keys=True).replace(' ', '')


@mark.parametrize(
    'current_cluster_name,resource_cluster,pre_cluster_instance,direct_route',
    [
     ('altc1-test-pre', 'altc1-test-pre',
      {
        '169.254.0.1_5000': {
            'value': j('169.254.0.1', 5000, 'altc1-test-pre'),
        }
      },
      {
        '169.254.0.2_5000': {
            'value': j('169.254.0.2', 5000, 'bar'),
        }
      }),
     ('test-pre', 'test-pre',
      {
          '169.254.0.1_5000': {
              'value': j('169.254.0.1', 5000, 'test-pre'),
          }
      },
      {
          '169.254.0.2_5000': {
              'value': j('169.254.0.2', 5000, 'bar'),
          }
      })]
)
def test_force_cluster_route(zk, long_poll, mocker, test_application_name,
                             current_cluster_name, resource_cluster,
                             pre_cluster_instance, direct_route):
    switch_status = True
    mocker.patch(
        'huskar_api.settings.FORCE_ROUTING_CLUSTERS',
        {'altc1-test-pre': 'altc1-test-pre', 'test-pre': 'test-pre'}
    )

    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return switch_status
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    zk.set(
        '/huskar/service/%s' % test_application_name,
        '{"default_route":{"overall":{"direct":"stable"}}}')
    zk.create(
        '/huskar/service/{}/{}'.format(
            test_application_name, resource_cluster
        ),
        '{"route":{"%s":"channel-stable-1"}}' % test_application_name,
        makepath=True
    )
    zk.create(
        '/huskar/service/%s/channel-stable-1' % test_application_name,
        '{"link":["bar"]}', makepath=True)
    zk.create(
        '/huskar/service/{}/{}/169.254.0.1_5000'.format(
            test_application_name,
            resource_cluster
        ),
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.0.2_5000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/baz/169.254.0.3_5000' % test_application_name,
        '{"ip":"169.254.0.3","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/config/%s/alpha/DB_URL' % test_application_name, 'mysql://',
        makepath=True)
    zk.create(
        '/huskar/switch/%s/stable/foo' % test_application_name, '0',
        makepath=True)
    zk.create(
        '/huskar/switch/%s/stable/bar' % test_application_name, '10.1',
        makepath=True)
    zk.create(
        '/huskar/switch/%s/overall/foo' % test_application_name, '20.1',
        makepath=True)
    # when use_route is ture, soa-route start, but hijack route failure,
    # use_route is false, soa-route close, but hijack route start(failure)
    queue = long_poll(
        service=['stable', 'direct', current_cluster_name,
                 'bar', 'baz', 'unknown'],
        use_route=True, current_cluster_name=current_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
       'service': {test_application_name: {
           'stable': {},
           'unknown': {},
           'direct': {'169.254.0.1_5000': {
               'value': j('169.254.0.1', 5000, resource_cluster),
           }},
           current_cluster_name: pre_cluster_instance,

           'bar': {'169.254.0.1_5000': {
               'value': j('169.254.0.1', 5000, resource_cluster),
           }},

           'baz': {'169.254.0.1_5000': {
               'value': j('169.254.0.1', 5000, resource_cluster),
           }},
       }},
       'switch': {
           test_application_name: {
               'stable': {
                   'foo': {u'value': u'0'},
                   'bar': {u'value': u'10.1'},
               }
           },
       },
       'config': {test_application_name: {
           'alpha': {'DB_URL': {u'value': u'mysql://'}}
       }},
       'service_info': {}
    }

    queue = long_poll(
        service=['direct', resource_cluster, 'bar', 'baz'],
        use_route=False, current_cluster_name=current_cluster_name)

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
       'service': {test_application_name: {
           'direct': pre_cluster_instance,
           resource_cluster: {'169.254.0.1_5000': {
               'value': j('169.254.0.1', 5000, resource_cluster),
           }},
           'bar': {'169.254.0.1_5000': {
               'value': j('169.254.0.1', 5000, resource_cluster),
           }},
           'baz': {'169.254.0.1_5000': {
               'value': j('169.254.0.1', 5000, resource_cluster),
           }},
       }},
       'switch': {
           test_application_name: {
               'stable': {
                   'foo': {u'value': u'0'},
                   'bar': {u'value': u'10.1'},
               }
           },
       },
       'config': {test_application_name: {
           'alpha': {'DB_URL': {u'value': u'mysql://'}}
       }},
       'service_info': {}
    }

    switch_status = False
    mocker.patch.object(switch, 'is_switched_on', fake_switch)

    # when use_route is ture, soa-route start, but hijack route failure,
    # use_route is false, soa-route close, but hijack route start(failure)
    queue = long_poll(
        service=['stable', 'direct', current_cluster_name, 'bar', 'baz'],
        use_route=True, current_cluster_name=current_cluster_name)

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'stable': {},
            'direct': direct_route,
            current_cluster_name: pre_cluster_instance,

            'bar': {'169.254.0.2_5000': {
                'value': j('169.254.0.2', 5000, 'bar'),
            }},

            'baz': {'169.254.0.3_5000': {
                'value': j('169.254.0.3', 5000, 'baz'),
            }},
        }},
        'switch': {
            test_application_name: {
                'stable': {
                    'foo': {u'value': u'0'},
                    'bar': {u'value': u'10.1'},
                }
            },
        },
        'config': {test_application_name: {
            'alpha': {'DB_URL': {u'value': u'mysql://'}}
        }},
        'service_info': {}
    }

    queue = long_poll(
        service=['direct', current_cluster_name, 'bar', 'baz'],
        use_route=False, current_cluster_name=current_cluster_name)

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'direct': {},
            current_cluster_name: pre_cluster_instance,
            'bar': {'169.254.0.2_5000': {
                'value': j('169.254.0.2', 5000, 'bar'),
            }},
            'baz': {'169.254.0.3_5000': {
                'value': j('169.254.0.3', 5000, 'baz'),
            }},
        }},
        'switch': {
            test_application_name: {
                'stable': {
                    'foo': {u'value': u'0'},
                    'bar': {u'value': u'10.1'},
                }
            },
        },
        'config': {test_application_name: {
            'alpha': {'DB_URL': {u'value': u'mysql://'}}
        }},
        'service_info': {}
    }


@mark.parametrize('current_cluster_name', ['altc1-test-pre', 'test-pre'])
def test_force_cluster_route_change(zk, long_poll, mocker,
                                    test_application_name,
                                    current_cluster_name):
    switch_status = True
    mocker.patch(
        'huskar_api.settings.FORCE_ROUTING_CLUSTERS',
        {'altc1-test-pre': 'altc1-test-pre', 'test-pre': 'test-pre'}
    )

    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return switch_status
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    zk.set(
        '/huskar/service/%s' % test_application_name,
        '{"default_route":{"overall":{"direct":"stable"}}}')
    zk.create(
        '/huskar/service/{}/{}'.format(
            test_application_name, current_cluster_name
        ),
        '{"route":{"%s":"channel-stable-1"}}' % test_application_name,
        makepath=True
    )
    zk.create(
        '/huskar/service/%s/channel-stable-1' % test_application_name,
        '{"link":["bar"]}', makepath=True)
    zk.create(
        '/huskar/service/{}/{}/169.254.0.1_5000'.format(
            test_application_name,
            current_cluster_name
        ),
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.0.2_5000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/baz/169.254.0.3_5000' % test_application_name,
        '{"ip":"169.254.0.3","port":{"main":5000}}', makepath=True)
    # when use_route is ture, soa-route start, but hijack route failure,
    # use_route is false, soa-route close, but hijack route start(failure)
    queue = long_poll(
        service=['channel-stable-1', current_cluster_name, 'bar', 'baz'],
        use_route=True, current_cluster_name=current_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    zk.set(
        '/huskar/service/{}/{}'.format(
            test_application_name, current_cluster_name
        ),
        '{"route":{"%s":"baz"}}' % test_application_name,
    )
    # no data because of callee cluster is not changed, still test-pre

    zk.create(
        '/huskar/service/%s/bar/169.254.0.4_5000' % test_application_name,
        '{"ip":"169.254.0.4","port":{"main":5000}}', makepath=True)

    # no data too

    zk.create(
        '/huskar/service/{}/{}/169.254.0.5_5000'.format(
            test_application_name,
            current_cluster_name
        ),
        '{"ip":"169.254.0.5","port":{"main":5000}}', makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {
        'service': {test_application_name: {
            'channel-stable-1': {'169.254.0.5_5000': {
                'value': j('169.254.0.5', 5000, current_cluster_name),
            }},  # direct -> channel-stable-1 -> bar
            current_cluster_name: {'169.254.0.5_5000': {
                'value': j('169.254.0.5', 5000, current_cluster_name),
            }},
            'bar': {'169.254.0.5_5000': {
                'value': j('169.254.0.5', 5000, current_cluster_name),
            }},

            'baz': {'169.254.0.5_5000': {
                'value': j('169.254.0.5', 5000, current_cluster_name),
            }},
        }},
    }
    zk.create(
        '/huskar/switch/%s/stable/bar' % test_application_name, '10.1',
        makepath=True)
    event = queue.get(timeout=5)
    assert event['body'] == {
        'switch': {
            test_application_name: {
                'stable': {
                    'bar': {u'value': u'10.1'},
                }
            },
        },
    }
    zk.create(
        '/huskar/config/%s/alpha/DB_URL' % test_application_name, 'mysql://',
        makepath=True)
    event = queue.get(timeout=5)
    assert event['body'] == {
        'config': {
            test_application_name: {
                'alpha': {'DB_URL': {u'value': u'mysql://'}}
            }
        },
    }


@mark.parametrize('data,error,status', [
    (None,
     'JSON payload must be present and match its schema',
     'BadRequest'),
    ({'service': None},
     '{"service": ["Field may not be null."]}',
     'ValidationError'),
    ({'service': {'base.foo': None}},
     '{"service": {"clusters": ["Field may not be null."]}}',
     'ValidationError'),
    ({'service': {'base.foo': [1]}},
     '{"service": {"clusters": {"0": ["Not a valid string."]}}}',
     'ValidationError'),
    ({'service': {'application-which-does-not-exist': []}},
     "application: application-which-does-not-exist doesn't exist",
     'ApplicationNotExistedError')
])
def test_bad_request(client, test_application_name, test_application_token,
                     data, error, status):
    r = client.post('/api/data/long_poll', data=json.dumps(data),
                    content_type='application/json',
                    headers={'Authorization': test_application_token})
    assert r.status_code == 400
    assert r.json['status'] == status
    assert r.json['message'] == error
    assert r.json['data'] is None


def test_bad_permission(client, test_application_name, test_token):
    r = client.post('/api/data/long_poll', data=json.dumps({
        'service': {test_application_name: []}}
    ), content_type='application/json', headers={
        'Authorization': test_token,
    })
    assert r.status_code == 200

    r = client.post('/api/data/long_poll', data=json.dumps({
        'config': {test_application_name: []}}
    ), content_type='application/json', headers={
        'Authorization': test_token,
    })
    assert r.status_code == 400
    assert r.json['status'] == 'NoAuthError'
    assert r.json['message'].endswith(
        'has no read authority on %s' % test_application_name)


def test_minimal_mode(zk, test_application_name, long_poll, minimal_mode):
    queue = long_poll(custom_payload={
        'service': {test_application_name: []},
    })
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {}},
        'switch': {},
        'config': {},
        'service_info': {},
    }


def test_create_config(zk, test_application_name, long_poll):
    queue = long_poll()

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {'stable': {}, 'foo': {}}},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {},
    }
    assert queue.empty()

    zk.create(
        '/huskar/config/%s/alpha/DB_URL' % test_application_name, 'mysql://',
        makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {'config': {
        test_application_name: {'alpha': {'DB_URL': {u'value': u'mysql://'}}},
    }}


def test_update_config(zk, test_application_name, long_poll):
    zk.create(
        '/huskar/config/%s/alpha/DB_URL' % test_application_name, 'mysql://',
        makepath=True)
    queue = long_poll()

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {'stable': {}, 'foo': {}}},
        'switch': {test_application_name: {'stable': {}}},
        'config': {
            test_application_name: {'alpha': {
                'DB_URL': {u'value': u'mysql://'},
            }},
        },
        'service_info': {}
    }

    zk.set(
        '/huskar/config/%s/alpha/DB_URL' % test_application_name, 'pgsql://')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {'config': {
        test_application_name: {'alpha': {'DB_URL': {u'value': u'pgsql://'}}}
    }}


def test_delete_config(zk, test_application_name, long_poll):
    zk.create(
        '/huskar/config/%s/alpha/DB_URL' % test_application_name, 'mysql://',
        makepath=True)
    queue = long_poll()

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {'stable': {}, 'foo': {}}},
        'switch': {test_application_name: {'stable': {}}},
        'config': {
            test_application_name: {'alpha': {
                'DB_URL': {u'value': u'mysql://'},
            }},
        },
        'service_info': {}
    }

    zk.delete('/huskar/config/%s/alpha/DB_URL' % test_application_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'delete'
    assert event['body'] == {'config': {
        test_application_name: {'alpha': {'DB_URL': {u'value': None}}}
    }}


def test_cluster_filter(zk, test_application_name, long_poll):
    zk.create(
        '/huskar/switch/%s/stable/foo' % test_application_name, '0',
        makepath=True)
    zk.create(
        '/huskar/switch/%s/stable/bar' % test_application_name, '10.1',
        makepath=True)
    zk.create(
        '/huskar/switch/%s/overall/foo' % test_application_name, '20.1',
        makepath=True)

    queue = long_poll()

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {'stable': {}, 'foo': {}}},
        'switch': {
            test_application_name: {
                'stable': {
                    'foo': {u'value': u'0'},
                    'bar': {u'value': u'10.1'},
                }
            },
        },
        'config': {test_application_name: {}},
        'service_info': {}
    }

    zk.set('/huskar/switch/%s/overall/foo' % test_application_name, '11.1')
    zk.set('/huskar/switch/%s/stable/foo' % test_application_name, '12.2')
    zk.set('/huskar/switch/%s/stable/bar' % test_application_name, '13.3')

    # overall/foo has been ignored
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {'switch': {test_application_name: {'stable': {
        'foo': {u'value': u'12.2'},
    }}}}

    # expected pushing order
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {'switch': {test_application_name: {'stable': {
        'bar': {u'value': u'13.3'},
    }}}}


def test_cluster_symlink_and_route(zk, test_application_name, long_poll):
    zk.set(
        '/huskar/service/%s' % test_application_name,
        '{"default_route":{"overall":{"direct":"stable"}}}')
    zk.create(
        '/huskar/service/%s/stable' % test_application_name,
        '{"route":{"%s":"channel-stable-1"}}' % test_application_name,
        makepath=True)
    zk.create(
        '/huskar/service/%s/channel-stable-1' % test_application_name,
        '{"link":["bar"]}', makepath=True)
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.0.2_5000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/baz/169.254.0.3_5000' % test_application_name,
        '{"ip":"169.254.0.3","port":{"main":5000}}', makepath=True)

    queue = long_poll(
        service=['stable', 'foo', 'direct', 'channel-stable-1'],
        switch=['stable'], config=['stable'], service_info=['stable'],
        use_route=True)

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'stable': {},
            'foo': {'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }},  # foo
            'direct': {'169.254.0.2_5000': {
                'value': j('169.254.0.2', 5000, 'bar'),
            }},  # direct -> channel-stable-1 -> bar
            'channel-stable-1': {'169.254.0.2_5000': {
                'value': j('169.254.0.2', 5000, 'bar'),
            }},  # channel-stable-1 -> bar
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {'stable': {}}},
        'service_info': {test_application_name: {'stable': {}}}
    }

    zk.create(
        '/huskar/service/%s/bar/169.254.0.2_6000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":6000}}', makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service'][test_application_name] == {
        'direct': {
            '169.254.0.2_6000': {
                'value': j('169.254.0.2', 6000, 'bar'),
            },
        },  # direct -> channel-stable-1 -> bar
        'channel-stable-1': {
            '169.254.0.2_6000': {
                'value': j('169.254.0.2', 6000, 'bar'),
            },
        },  # channel-stable-1 -> bar
    }

    zk.set(
        '/huskar/service/%s/bar/169.254.0.2_6000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":6001}}')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service'][test_application_name] == {
        'direct': {
            '169.254.0.2_6000': {
                'value': j('169.254.0.2', 6001, 'bar'),
            },
        },  # direct -> channel-stable-1 -> bar
        'channel-stable-1': {
            '169.254.0.2_6000': {
                'value': j('169.254.0.2', 6001, 'bar'),
            },
        },  # channel-stable-1 -> bar
    }

    zk.set(
        '/huskar/service/%s/stable' % test_application_name,
        '{"route":{"unknown":"channel-stable-2"}}')
    # We don't care about the cluster migration in this test case.
    event = queue.get(timeout=5)
    assert event['message'] == 'all'

    # The instance creation should be notified by the application default route
    zk.create(
        '/huskar/service/%s/stable/169.254.0.3_6000' % test_application_name,
        '{"ip":"169.254.0.3","port":{"main":6000}}', makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service'][test_application_name] == {
        'direct': {
            '169.254.0.3_6000': {
                'value': j('169.254.0.3', 6000, 'stable'),
            },
        },  # direct -> stable (default)
        'stable': {
            '169.254.0.3_6000': {
                'value': j('169.254.0.3', 6000, 'stable'),
            },
        },  # stable
    }

    # Change the application default route
    zk.set('/huskar/service/%s' % test_application_name, '{"broken-json')
    # We don't care about the cluster migration in this test case.
    event = queue.get(timeout=5)
    assert event['message'] == 'all'

    # The instance creation should be notified by the global default route
    zk.set(
        '/huskar/service/%s/bar/169.254.0.2_6000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":6002}}')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service'][test_application_name] == {
        'direct': {
            '169.254.0.2_6000': {
                'value': j('169.254.0.2', 6002, 'bar'),
            },
        },  # direct -> channel-stable-1 (default) -> bar
        'channel-stable-1': {
            '169.254.0.2_6000': {
                'value': j('169.254.0.2', 6002, 'bar'),
            },
        },  # channel-stable-1 -> bar
    }


def test_cluster_symlink_without_route(zk, test_application_name, long_poll):
    zk.create(
        '/huskar/service/%s/stable' % test_application_name,
        '{"route":{"%s":"channel-stable-1"}}' % test_application_name,
        makepath=True)
    zk.create(
        '/huskar/service/%s/channel-stable-1' % test_application_name,
        '{"link":["bar"]}', makepath=True)
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.0.2_5000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/baz/169.254.0.3_5000' % test_application_name,
        '{"ip":"169.254.0.3","port":{"main":5000}}', makepath=True)

    queue = long_poll(
        service=['stable', 'foo', 'direct', 'channel-stable-1'],
        switch=['stable'], config=['stable'], service_info=['stable'],
        use_route=False)

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'stable': {},
            'direct': {},
            'foo': {'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }},  # foo
            'channel-stable-1': {'169.254.0.2_5000': {
                'value': j('169.254.0.2', 5000, 'bar'),
            }},  # channel-stable-1 -> bar
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {'stable': {}}},
        'service_info': {test_application_name: {'stable': {}}}
    }

    zk.create(
        '/huskar/service/%s/bar/169.254.0.2_6000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":6000}}', makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service'][test_application_name] == {
        'channel-stable-1': {
            '169.254.0.2_6000': {
                'value': j('169.254.0.2', 6000, 'bar'),
            },
        },  # channel-stable-1 -> bar
    }

    zk.set(
        '/huskar/service/%s/bar/169.254.0.2_6000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":6001}}')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service'][test_application_name] == {
        'channel-stable-1': {
            '169.254.0.2_6000': {
                'value': j('169.254.0.2', 6001, 'bar'),
            },
        },  # channel-stable-1 -> bar
    }


def test_cluster_multiplex_symlink(zk, test_application_name, long_poll):
    zk.create(
        '/huskar/service/%s/channel-stable-1' % test_application_name,
        '{"link":["foo", "bar"]}', makepath=True)
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.0.2_5000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":5000}}', makepath=True)

    queue = long_poll(
        service=['channel-stable-1'], switch=[], config=[], use_route=False)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'channel-stable-1': {
                '169.254.0.1_5000': {
                    'value': j('169.254.0.1', 5000, 'foo'),
                },  # foo
                '169.254.0.2_5000': {
                    'value': j('169.254.0.2', 5000, 'bar'),
                },  # bar
            },
        }},
        'switch': {test_application_name: {}},
        'config': {test_application_name: {}},
        'service_info': {},
    }

    zk.set(
        '/huskar/service/%s/bar/169.254.0.2_5000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":5001}}')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {'service': {test_application_name: {
        'channel-stable-1': {
            '169.254.0.2_5000': {
                'value': j('169.254.0.2', 5001, 'bar'),
            },  # bar
        },
    }}}

    zk.delete(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'delete'
    assert event['body'] == {'service': {test_application_name: {
        'channel-stable-1': {
            '169.254.0.1_5000': {u'value': None},  # foo
        },
    }}}

    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_6000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":6000}}')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {'service': {test_application_name: {
        'channel-stable-1': {
            '169.254.0.1_6000': {
                'value': j('169.254.0.1', 6000, 'foo'),
            },  # foo
        },
    }}}

    zk.set(
        '/huskar/service/%s/channel-stable-1' % test_application_name,
        '{"link":["foo", "baz"]}')
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            # bar is detached and baz is empty
            'channel-stable-1': {
                '169.254.0.1_6000': {
                    'value': j('169.254.0.1', 6000, 'foo'),
                },  # foo
            },
        }},
        'switch': {test_application_name: {}},
        'config': {test_application_name: {}},
        'service_info': {}
    }


def test_cluster_migration(zk, test_application_name, long_poll):
    zk.set(
        '/huskar/service/%s' % test_application_name,
        '{"default_route":{"overall":{"direct":"stable"}}}')
    zk.create(
        '/huskar/service/%s/stable' % test_application_name,
        '{"link":["bar"],"route":{"%s":"baz"}}' % test_application_name,
        makepath=True)
    zk.create(
        '/huskar/service/%s/stable/169.254.0.0_5000' % test_application_name,
        '{"ip":"169.254.0.0","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_6000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":6000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.0.2_5000' % test_application_name,
        '{"ip":"169.254.0.2","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/baz' % test_application_name,
        '{"link":["bar"]}', makepath=True)

    queue = long_poll(
        service=['stable', 'foo', 'direct'], switch=['stable'],
        config=['stable'], use_route=True)

    instances_stable = {
        '169.254.0.0_5000': {
            'value': j('169.254.0.0', 5000, 'stable'),
        },
    }
    instances_foo = {
        '169.254.0.1_5000': {
            'value': j('169.254.0.1', 5000, 'foo'),
        },
        '169.254.0.1_6000': {
            'value': j('169.254.0.1', 6000, 'foo'),
        },
    }
    instances_bar = {
        '169.254.0.2_5000': {
            'value': j('169.254.0.2', 5000, 'bar'),
        },
    }

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_bar,  # stable -> bar
        'foo': instances_foo,     # foo
        'direct': instances_bar,  # direct -> baz -> bar
    }

    zk.set(
        '/huskar/service/%s/stable' % test_application_name,
        '{"link":["foo"],"route":{"%s":"baz"}}' % test_application_name)

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_foo,  # stable -> foo
        'foo': instances_foo,     # foo
        'direct': instances_bar,  # direct -> baz -> bar
    }

    zk.set(
        '/huskar/service/%s/baz' % test_application_name,
        '{"link":["foo"]}')

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_foo,  # stable -> foo
        'foo': instances_foo,     # foo
        'direct': instances_foo,  # direct -> baz -> foo
    }

    zk.set(
        '/huskar/service/%s/stable' % test_application_name,
        '{"link":["foo"],"route":{"%s":"bar"}}' % test_application_name)

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_foo,  # stable -> foo
        'foo': instances_foo,     # foo
        'direct': instances_bar,  # direct -> bar
    }

    zk.set(
        '/huskar/service/%s/stable' % test_application_name,
        '{"link":["foo"],"route":{"unknown-app":"bar"}}')

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_foo,  # stable -> foo
        'foo': instances_foo,     # foo
        'direct': instances_foo,  # direct -> foo
    }

    # This action make the default route be activated.
    zk.set(
        '/huskar/service/%s/stable' % test_application_name,
        '{"route":{"unknown-app":"bar"}}')

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_stable,  # stable
        'foo': instances_foo,        # foo
        'direct': instances_stable,  # direct -> stable
    }

    # This action should not trigger any route change.
    # It is not the route node of current cluster.
    zk.set(
        '/huskar/service/%s/foo' % test_application_name,
        '{"route":{"%s":"bar"}}' % test_application_name)
    assert queue.empty()


def test_cluster_migration_with_default_route(
        zk, long_poll, test_application_name):
    zk.set(
        '/huskar/service/%s' % test_application_name,
        '{"default_route":{"overall":{"direct":"stable"}}}')
    zk.create(
        '/huskar/service/%s/stable' % test_application_name, '', makepath=True)
    zk.create(
        '/huskar/service/%s/stable/169.254.0.0_5000' % test_application_name,
        '{"ip":"169.254.0.0","port":{"main":5000}}', makepath=True)

    instances_stable = {
        '169.254.0.0_5000': {
            'value': j('169.254.0.0', 5000, 'stable'),
        },
    }

    queue = long_poll(
        service=['stable', 'direct', 'unknown'], switch=[], config=[],
        use_route=True)

    # The new created session should have default route too.
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_stable,  # stable            (physical cluster)
        'direct': instances_stable,  # direct -> stable  (default route)
        'unknown': {},               # unknown           (does not exist)
    }

    zk.set(
        '/huskar/service/%s/stable' % test_application_name,
        '{"route":{"%s":"unknown"}}' % test_application_name)

    # The default route is broken
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_stable,  # stable            (physical cluster)
        'direct': {},                # direct -> unknown (default route)
        'unknown': {},               # unknown           (does not exist)
    }

    zk.set(
        '/huskar/service/%s/stable' % test_application_name,
        '{"route":{"%s":null}}' % test_application_name)

    # The default route is back
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_stable,  # stable            (physical cluster)
        'direct': instances_stable,  # direct -> stable  (default route)
        'unknown': {},               # unknown           (does not exist)
    }

    zk.set('/huskar/service/%s' % test_application_name, '')

    # The default route is changed
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': instances_stable,  # stable            (physical cluster)
        'direct': {},                # direct -> channel-stable-1 (default)
        'unknown': {},               # unknown           (does not exist)
    }


def test_keys_with_slash(zk, test_application_name, long_poll):
    zk.create(
        '/huskar/config/%s/alpha/BAR%%SLASH%%BAZ' % test_application_name,
        'FOO', makepath=True)
    queue = long_poll()

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {
            test_application_name: {'stable': {}, 'foo': {}},
        },
        'switch': {
            test_application_name: {'stable': {}},
        },
        'config': {
            test_application_name: {'alpha': {'BAR/BAZ': {u'value': u'FOO'}}},
        },
        'service_info': {}
    }

    assert queue.empty()

    zk.create(
        '/huskar/config/%s/alpha/FOO%%SLASH%%' % test_application_name, 'BAR',
        makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {'config': {
        test_application_name: {'alpha': {'FOO/': {u'value': u'BAR'}}},
    }}


def test_tree_broadcast(zk, long_poll, test_application_name):
    zk.create('/huskar/config/%s/alpha/foo' % test_application_name, 'FOO',
              makepath=True)

    queue_a = long_poll()
    queue_b = long_poll()

    for queue in queue_a, queue_b:
        event = queue.get(timeout=5)
        assert event['message'] == 'all'
        assert event['body']['config'] == {
            test_application_name: {'alpha': {'foo': {'value': 'FOO'}}},
        }
        assert queue.empty()

    zk.create('/huskar/config/%s/alpha/bar' % test_application_name, 'BAR',
              makepath=True)

    for queue in queue_a, queue_b:
        event = queue.get(timeout=5)
        assert event['message'] == 'update'
        assert event['body']['config'] == {
            test_application_name: {'alpha': {'bar': {'value': 'BAR'}}},
        }
        assert queue.empty()


def test_tree_timeout(
        mocker, client, test_application_name, test_application_token):
    url = '/api/data/long_poll'
    data = {'service': {test_application_name: []}}

    original_start = TreeHolder.start
    mocker.patch.object(TreeHolder, 'start', autospec=True)
    mocker.patch.object(settings, 'ZK_SETTINGS', {'treewatch_timeout': 1})

    with raises(TreeTimeoutError):
        client.post(
            url, content_type='application/json', data=json.dumps(data),
            headers={'Authorization': test_application_token})

    mocker.patch.object(TreeHolder, 'start', original_start)
    r = client.post(
        url, content_type='application/json', data=json.dumps(data),
        headers={'Authorization': test_application_token})
    assert r.status_code == 200, r.data
    r.close()  # Drop established session


def test_tree_timeout_cause_http_500(
        mocker, app, client, test_application_name, test_application_token):
    # The TreeTimeoutError should be raised as HTTP 500 instead of being
    # hidden in the response stream.
    app.config['PROPAGATE_EXCEPTIONS'] = False

    url = '/api/data/long_poll'
    data = {'service': {test_application_name: []}}

    mocker.patch.object(TreeHolder, 'start', autospec=True)
    mocker.patch.object(settings, 'ZK_SETTINGS', {'treewatch_timeout': 1})

    r = client.post(
        url, content_type='application/json', data=json.dumps(data),
        headers={'Authorization': test_application_token})
    assert r.status_code == 500, r.data
    assert r.json['status'] == 'InternalServerError'


def test_issue_147_regression(zk, test_application_name, long_poll):
    zk.create(
        '/huskar/service/%s/stable' % test_application_name,
        '{"link":[]}', makepath=True)
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)

    queue = long_poll()

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'stable': {},
            'foo': {'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }},
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {}
    }

    zk.set(
        '/huskar/service/%s/stable' % test_application_name,
        '{"link":["foo"]}')

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'stable': {'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }},
            'foo': {'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }},
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {}
    }


@mark.parametrize('dirty_data', ['{}', '1', 'xxx', '[]'])
def test_issue_171_regression(
        zk, test_application_name, long_poll, dirty_data):
    """We should be fault-tolerance while resolving symlink."""
    zk.create(
        '/huskar/service/%s/stable' % test_application_name,
        dirty_data, makepath=True)
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)

    queue = long_poll(service=['stable'])
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {'stable': {}}},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {}
    }

    queue = long_poll(service=['stable', 'foo'])
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'stable': {},
            'foo': {'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }},
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {}
    }


def test_jira_842_regression(zk, test_application_name, long_poll):
    """Fix http://jira.ele.to:8088/browse/FXBUG-842.

    The cluster unlink events should be published.
    """
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.1.1_5000' % test_application_name,
        '{"ip":"169.254.1.1","port":{"main":5000}}', makepath=True)
    zk.create('/huskar/service/%s/stable' % test_application_name,
              '{"link":["foo"]}', makepath=True)

    queue = long_poll(service=['stable', 'bar'])

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'stable': {'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }},
            'bar': {'169.254.1.1_5000': {
                'value': j('169.254.1.1', 5000, 'bar'),
            }},
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {}
    }

    zk.delete('/huskar/service/%s/stable' % test_application_name)

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'stable': {},
            'bar': {'169.254.1.1_5000': {
                'value': j('169.254.1.1', 5000, 'bar'),
            }},
        }},
        'switch': {test_application_name: {
            'stable': {},
        }},
        'config': {test_application_name: {}},
        'service_info': {}
    }, 'The "stable" cluster should disappear.'


def test_jira_844_regression(zk, test_application_name, long_poll):
    """Fix http://jira.ele.to:8088/browse/FXBUG-844.

    The subtree of symlink cluster should not be published, but overrided by
    the subtree of target cluster.
    """
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.1.1_5000' % test_application_name,
        '{"ip":"169.254.1.1","port":{"main":5000}}', makepath=True)

    queue = long_poll(service=['foo'])
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'foo': {'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }},
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {}
    }

    zk.set(
        '/huskar/service/%s/foo' % test_application_name, '{"link":["bar"]}')
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {
            'foo': {'169.254.1.1_5000': {
                'value': j('169.254.1.1', 5000, 'bar'),
            }},
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {}
    }, '"169.254.0.1_5000" should not appear'

    zk.set(
        '/huskar/service/%s/bar/169.254.1.1_5000' % test_application_name, '')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {'service': {test_application_name: {
        'foo': {'169.254.1.1_5000': {'value': ''}},
    }}}


def test_jira_845_regression_1(zk, test_application_name, long_poll):
    """Fix http://jira.ele.to:8088/browse/FXBUG-845."""
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)

    queue = long_poll()

    event = queue.get(timeout=5)
    assert event['message'] == 'all'

    zk.create('/huskar/service/%s/beta' % test_application_name,
              '{"link":["foo"]}', makepath=True)
    zk.create(
        '/huskar/service/%s/foo/169.254.0.2_5000' % test_application_name,
        '', makepath=True)

    event = queue.get(timeout=5)
    assert event['message'] != 'all', 'We should be silent for cluster linking'
    assert queue.empty(), 'We should notify for node creation only'


def test_jira_845_regression_2(zk, test_application_name, long_poll):
    zk.create(
        '/huskar/service/%s/foo/169.254.0.1_5000' % test_application_name,
        '{"ip":"169.254.0.1","port":{"main":5000}}', makepath=True)
    zk.create(
        '/huskar/service/%s/bar/169.254.1.1_5000' % test_application_name,
        '{"ip":"169.254.1.1","port":{"main":5000}}', makepath=True)

    foo_queue = long_poll(service=['foo'])
    bar_queue = long_poll(service=['bar'])
    assert foo_queue.get(timeout=5)['message'] == 'all'
    assert bar_queue.get(timeout=5)['message'] == 'all'

    zk.set(
        '/huskar/service/%s/bar' % test_application_name, '{"link":["foo"]}')

    bar_event = bar_queue.get(timeout=5)
    assert bar_event['message'] == 'all'
    assert bar_event['body'] == {
        'service': {test_application_name: {
            'bar': {u'169.254.0.1_5000': {
                'value': j('169.254.0.1', 5000, 'foo'),
            }}
        }},
        'switch': {test_application_name: {'stable': {}}},
        'config': {test_application_name: {}},
        'service_info': {}
    }

    assert foo_queue.empty()


@mark.parametrize('data', [{'service': {}}, {'config': {}}, {'switch': {}}])
def test_issue_358_regression(request, client, test_application_name,
                              test_application_token, data):
    """The empty subscription should be acceptable. """
    r = client.post(
        '/api/data/long_poll', content_type='application/json',
        data=json.dumps(data), query_string={'life_span': 0},
        headers={'Authorization': test_application_token})
    assert r.status_code == 200, r.data


def test_session_with_max_lifetime(
        zk, test_application_name, long_poll, mocker):
    mocker.patch.object(settings, 'LONG_POLLING_LIFE_SPAN_JITTER', 10)
    queue = long_poll(life_span=1)
    queue.get(timeout=5)
    gevent.sleep(1.1)

    zk.create(
        '/huskar/config/%s/alpha/DB_URL' % test_application_name, 'mysql://',
        makepath=True)

    with raises(Empty):
        queue.get(timeout=1.1)


def test_request_data_include_useless_keys(long_poll, test_application_name):
    queue = long_poll(custom_payload={
        'life_span': 0,
        'service': {
            test_application_name: ['stable']
        },
        'trigger': 1
    })

    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body'] == {
        'service': {test_application_name: {'stable': {}}},
        'switch': {},
        'config': {},
        'service_info': {}
    }

    assert queue.empty()


@fixture
def make_cluster(zk):
    def make(application_name, cluster_name, cluster_info, instance_names):
        path = '/huskar/service/%s/%s' % (application_name, cluster_name)
        zk.ensure_path(path)
        zk.set(path, value=cluster_info, version=-1)
        for name in instance_names:
            subpath = '%s/%s' % (path, name)
            zk.ensure_path(subpath)
            zk.set(subpath, value=b'{}', version=-1)

    return make


@fixture
def hijack_context(mocker, zk, test_application_name, make_cluster,
                   dest_application_name):
    from_application_name = test_application_name
    from_cluster_name = 'foobar'

    logger = mocker.patch(
        'huskar_api.models.route.hijack.logger', autospec=True)

    zk.create(
        '/huskar/service/%s/stable/169.254.0.1_1000' % test_application_name,
        '{}', makepath=True)

    zk.create(
        '/huskar/service/%s/stable/192.168.0.1_1000' % dest_application_name,
        '{}', makepath=True)

    rm = RouteManagement(
        huskar_client, from_application_name, from_cluster_name)
    make_cluster(test_application_name, 'stable1', '{}', ['233'])
    rm.set_route(test_application_name, 'stable1')

    make_cluster(dest_application_name, 'stable1', '{}', ['233'])
    rm.set_route(dest_application_name, 'stable1')

    def j(cluster_name):
        return {'value': '{}'}

    return from_application_name, from_cluster_name, logger, j


@mark.parametrize('force_routing_cluster', [False, True])
def test_hijack_route_in_disabled_mode(
        mocker, zk, hijack_context, test_application_name,
        long_poll, make_cluster, force_routing_cluster):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return force_routing_cluster
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    from_application_name, from_cluster_name, logger, j = hijack_context

    # Enabled but not learned
    mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
        test_application_name: 'H'})

    queue = long_poll(service=['stable'])
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable')},
    }
    logger.info.assert_called_with(
        'Skip: %s %s %s', from_application_name, '127.0.0.1', None)

    # from_cluster but not enabled
    mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
        test_application_name: 'X'})  # Invalid option

    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable')},
    }
    logger.warning.assert_called_with(
        'Invalid hijack mode: %s', from_application_name)

    # Learned and enabled but switched off
    mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
        test_application_name: 'E'})

    def is_switched_on(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_HIJACK:
            return False
        return default
    mocker.patch.object(switch, 'is_switched_on', is_switched_on)

    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable')},
    }

    # force enable
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', {test_application_name})
    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'233': j('stable1')},
        'direct': {'233': j('stable1')},
    }

    # force enable but exclude -> disable
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
            test_application_name: [from_application_name]})

    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable')},
    }
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {})
    # force enable but exclude with matched * -> disable
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
            test_application_name[:-3] + '*': [from_application_name]})

    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable')},
    }

    # force enable with exclude * but equal exclude first -> enable
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
            test_application_name[:-3] + '*': [from_application_name],
            test_application_name: []})
    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'233': j('stable1')},
        'direct': {'233': j('stable1')},
    }

    # force enable with unmatched exclude * -> enable
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
            '233_*': [from_application_name]})
    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'233': j('stable1')},
        'direct': {'233': j('stable1')},
    }

    # force enable with unmatched exclude -> enable
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
            test_application_name[:-3]: [from_application_name]})
    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'233': j('stable1')},
        'direct': {'233': j('stable1')},
    }

    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', set())
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {})


@mark.parametrize('force_routing_cluster', [False, True])
def test_hijack_route_in_checking_mode(
        mocker, zk, hijack_context, test_application,
        test_application_name, long_poll, make_cluster, force_routing_cluster):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return force_routing_cluster
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    from_application_name, from_cluster_name, logger, j = hijack_context
    mocker.patch.object(settings, 'ROUTE_DOMAIN_EZONE_MAP', {
        '127.0.0.1': 'alta1',
        'localhost': 'alta1',
    })
    mocker.patch.object(settings, 'ROUTE_EZONE_DEFAULT_HIJACK_MODE', {
        'alta1': 'S',
    })
    mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
        test_application_name: 'C'})

    # Unstable route found
    queue = long_poll(service=['stable', 'stable1'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable')},
        'stable1': {'233': j('stable1')},
    }

    intent_map = {'direct': {'stable', 'stable1'}}
    logger.info.assert_called_with(
        '[%s]Unstable: %s %s -> %s %s',
        'C', from_application_name, from_cluster_name,
        test_application_name, intent_map)

    # Mismatch route found, because unmatch route
    logger.reset_mock()
    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable')},
    }

    intent_map = {
        'direct': {'stable'}}
    logger.info.assert_has_calls([
        mocker.call(
            '[%s]Mismatch: %s %s -> %s %s %s %s', 'C', from_application_name,
            from_cluster_name, test_application_name, intent_map,
            'stable1', 'stable'),
        mocker.call(
            '[%s]Unexpected mismatch: %s %s -> %s %s %s %s', 'C',
            from_application_name,
            from_cluster_name, test_application_name, intent_map,
            'stable1', 'stable'),
    ], any_order=True)

    # Mismatch route found, because empty cluster
    logger.reset_mock()
    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable')},
    }

    intent_map = {
        'direct': {'stable'}}
    logger.info.assert_has_calls([
        mocker.call(
            '[%s]Mismatch: %s %s -> %s %s %s %s', 'C', from_application_name,
            from_cluster_name, test_application_name, intent_map,
            'stable1', 'stable'),
        mocker.call(
            '[%s]Unexpected mismatch: %s %s -> %s %s %s %s', 'C',
            from_application_name,
            from_cluster_name, test_application_name, intent_map,
            'stable1', 'stable'),
    ], any_order=True)

    # Mismatch route, but dest_cluster_name in ROUTE_DEST_CLUSTER_BLACKLIST
    logger.reset_mock()
    mocker.patch.object(settings, 'ROUTE_DEST_CLUSTER_BLACKLIST', {
        test_application_name: ['alta1-stable'],
    })
    queue = long_poll(service=['alta1-stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'alta1-stable': {},
    }
    logger.info.assert_not_called()
    mocker.patch.object(settings, 'ROUTE_DEST_CLUSTER_BLACKLIST', {})

    # dest_application in LEGACY_APPLICATION_LIST
    logger.reset_mock()
    mocker.patch('huskar_api.settings.LEGACY_APPLICATION_LIST',
                 [test_application_name + '1s'])
    Application.create(test_application_name + '1s', test_application.team_id)
    queue = long_poll(
        custom_payload={'service': {
            test_application_name + '1s': ['stable'],
        }},
        current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name + '1s'] == {
        'stable': {},
    }

    logger.info.assert_not_called()

    # force enable
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_DEST_APPS',
        {test_application_name + '1s'})
    queue = long_poll(
        custom_payload={'service': {
            test_application_name + '1s': ['stable'],
        }})
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name + '1s'] == {
        'stable': {},
        'direct': {},
    }

    # force enable but exclude
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
            test_application_name + '1s': [from_application_name]})
    queue = long_poll(
        custom_payload={'service': {
            test_application_name + '1s': ['stable'],
        }}, current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name + '1s'] == {
        'stable': {},
    }
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {})

    # force enable but exclude with *
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
            test_application_name + '1*': [from_application_name]})
    queue = long_poll(
        custom_payload={'service': {
            test_application_name + '1s': ['stable'],
        }})
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name + '1s'] == {
        'stable': {},
    }

    mocker.patch('huskar_api.settings.LEGACY_APPLICATION_LIST', [])
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', set())
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {})


@mark.parametrize('force_routing_cluster', [False, True])
def test_hijack_route_enable_mode_sentry_message(
        mocker, zk, hijack_context, test_application, force_routing_cluster,
        test_application_name, long_poll, make_cluster):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return force_routing_cluster
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    from_application_name, from_cluster_name, logger, j = hijack_context
    capture_message = mocker.patch(
        'huskar_api.models.route.hijack.capture_message', autospec=True)

    mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
        test_application_name: 'E'})

    rm = RouteManagement(
        huskar_client, from_application_name, from_cluster_name)
    make_cluster(test_application_name, 'stable', '{}', ['233'])
    rm.set_route(test_application_name, 'stable')

    # Unstable route
    queue = long_poll(service=['stable', 'stable1'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'direct': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        'stable1': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
    }

    intent_map = {'direct': {u'stable', u'stable1'}}
    logger.info.assert_has_calls([mocker.call(
        '[%s]Unstable: %s %s -> %s %s',
        'E', from_application_name, from_cluster_name,
        test_application_name, intent_map)])
    capture_message.assert_called_once_with(
        '[E]RouteHijack unstable',
        extra={
            'from_application_name': from_application_name,
            'from_cluster_name': from_cluster_name,
            'application_name': test_application_name,
            'intent_map': repr(intent_map),
            'intent': 'direct',
        })


@mark.parametrize('hijack_mode,force,exclude,empty_item', [
    ('E', False, '', {'stable': {}, 'direct': {}}),
    ('E', False, 'A', {'stable': {}, 'direct': {}}),
    ('E', False, '*', {'stable': {}, 'direct': {}}),
    ('E', True, '', {'stable': {}, 'direct': {}}),
    ('E', True, 'A', {'stable': {}, 'direct': {}}),
    ('E', True, '*', {'stable': {}, 'direct': {}}),
    ('S', False, '', {'stable': {}, 'direct': {}}),
    ('S', False, 'A', {'stable': {}, 'direct': {}}),
    ('S', False, '*', {'stable': {}, 'direct': {}}),
    ('S', True, '', {'stable': {}, 'direct': {}}),
    ('S', True, 'A', {'stable': {}, 'direct': {}}),
    ('S', True, '*', {'stable': {}, 'direct': {}}),
    ('', False, '', {'stable': {}, 'direct': {}}),
    ('', False, 'A', {'stable': {}, 'direct': {}}),
    ('', False, '*', {'stable': {}, 'direct': {}}),
    ('', True, '', {'stable': {}, 'direct': {}}),
    ('', True, 'A', {'stable': {}, 'direct': {}}),
    ('', True, '*', {'stable': {}, 'direct': {}}),
    ('D', True, '', {'stable': {}, 'direct': {}}),
    ('C', True, '', {'stable': {}, 'direct': {}}),
])
@mark.parametrize('ezone', [None, 'alta1'])
@mark.parametrize('force_routing_cluster', [False, True])
def test_hijack_route_enable(
        mocker, zk, hijack_context, test_application, test_application_name,
        long_poll, hijack_mode, force, exclude,
        empty_item, make_cluster, ezone, force_routing_cluster):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return force_routing_cluster
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    from_application_name, from_cluster_name, logger, j = hijack_context

    if hijack_mode:
        mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
            test_application_name: hijack_mode})
    else:
        mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {})
        mocker.patch.object(settings, 'EZONE', ezone)
        hijack_config = {
            'default': 'S',
            'lalala': 'D',
        }
        if ezone:
            hijack_config[ezone] = 'S'
        mocker.patch.object(settings, 'ROUTE_EZONE_DEFAULT_HIJACK_MODE',
                            hijack_config)
    if force:
        mocker.patch.object(
            settings, 'ROUTE_FORCE_ENABLE_DEST_APPS',
            {test_application_name, test_application_name + '1s'})
    if exclude:
        if exclude == '*':
            mocker.patch.object(
                settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
                    test_application_name: [from_application_name],
                    test_application_name + '1*': [from_application_name]})
        else:
            mocker.patch.object(
                settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {
                    test_application_name: [from_application_name],
                    test_application_name + '1s': [from_application_name]})

    # Hijack
    rm = RouteManagement(
        huskar_client, from_application_name, from_cluster_name)
    make_cluster(test_application_name, 'stable', '{}', ['233'])
    rm.set_route(test_application_name, 'stable')

    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        'direct': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
    }

    # Hijack and migrate cluster

    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        'direct': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
    }

    intent_map = {'direct': {'stable'}}
    logger.info.assert_called_with(
        'Hijack: %s %s -> %s %s', from_application_name, from_cluster_name,
        test_application_name, intent_map)

    # Hijack and register service
    zk.create(
        ('/huskar/service/%s/' % test_application_name) +
        ('stable/169.254.0.3_1000'),
        '{}', makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.3_1000': j('stable')},
        'direct': {'169.254.0.3_1000': j('stable')},
    }

    # The switch should not be hijacked
    zk.create(
        '/huskar/switch/%s/stable/USE_A' % test_application_name, '50',
        makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['switch'][test_application_name] == {
        'stable': {'USE_A': {'value': '50'}}}

    # Multiple applications are okay too
    Application.create(test_application_name + '1s', test_application.team_id)
    queue = long_poll(custom_payload={'service': {
        test_application_name: ['stable'],
        test_application_name + '1s': ['stable'],
    }}, current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'] == {
        test_application_name: {
            'stable': {
                '169.254.0.1_1000': j('stable'),
                '169.254.0.3_1000': j('stable'),
                '233': j('stable'),
            },
            'direct': {
                '169.254.0.1_1000': j('stable'),
                '169.254.0.3_1000': j('stable'),
                '233': j('stable'),
            },
        },
        test_application_name + '1s': empty_item,
    }

    # Multiple applications but touch one of them only
    zk.delete(
        ('/huskar/service/%s/' % test_application_name) +
        ('stable/169.254.0.3_1000'))
    event = queue.get(timeout=5)
    assert event['message'] == 'delete'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.3_1000': {'value': None}},
        'direct': {'169.254.0.3_1000': {'value': None}},
    }

    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', set())
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', {})


@mark.parametrize('force_routing_cluster', [False, True])
def test_skip_hijack_route(
        mocker, zk, hijack_context, test_application, test_application_name,
        long_poll, make_cluster, force_routing_cluster):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return force_routing_cluster
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    from_application_name, from_cluster_name, logger, j = hijack_context

    mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
        test_application_name: 'E'})

    # Hijack to route mode
    rm = RouteManagement(
        huskar_client, from_application_name, from_cluster_name)
    make_cluster(test_application_name, 'stable', '{}', ['233'])
    rm.set_route(test_application_name, 'stable')

    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        'direct': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
    }

    # Skip hijack to route mode, from_application_name in
    # LEGACY_APPLICATION_LIST
    logger.reset_mock()
    mocker.patch.object(settings, 'LEGACY_APPLICATION_LIST',
                        [from_application_name])
    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
    }
    logger.info.assert_called_with(
        'Skip: %s %s %s',
        from_application_name, '127.0.0.1', from_cluster_name)
    logger.reset_mock()

    # dest application force enabled, hijack route
    mocker.patch.object(
        settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', {test_application_name})
    queue = long_poll(service=['stable'],
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'][test_application_name] == {
        'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        'direct': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
    }

    mocker.patch.object(settings, 'LEGACY_APPLICATION_LIST', [])
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', set())


@mark.parametrize('hijack_mode', ['', 'C', 'D', '233'])
@mark.parametrize('force_routing_cluster', [False, True])
def test_force_enable_route_for_dest_application(
        mocker, zk, hijack_context, test_application_name,
        dest_application_name, long_poll, make_cluster,
        test_application, hijack_mode, force_routing_cluster):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return force_routing_cluster
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', set())
    from_application_name, from_cluster_name, logger, j = hijack_context

    mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
        test_application_name: hijack_mode})

    rm = RouteManagement(
        huskar_client, from_application_name, from_cluster_name)
    make_cluster(dest_application_name, 'stable', '{}', ['233'])
    make_cluster(test_application_name, 'stable', '{}', ['233'])
    rm.set_route(dest_application_name, 'stable')
    rm.set_route(test_application_name, 'stable')
    custom_payload = {'service': {
        test_application_name: ['stable'],
        dest_application_name: ['stable'],
    }}

    # default: disable
    queue = long_poll(custom_payload=custom_payload,
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'] == {
        dest_application_name: {
            'stable': {'192.168.0.1_1000': j('stable'), '233': j('stable')},
        },
        test_application_name: {
            'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        },
    }

    # Multiple applications, force one
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS',
                        {dest_application_name})
    queue = long_poll(custom_payload=custom_payload,
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'] == {
        dest_application_name: {
            'stable': {'192.168.0.1_1000': j('stable'), '233': j('stable')},
            'direct': {'192.168.0.1_1000': j('stable'), '233': j('stable')},
        },
        test_application_name: {
            'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        },
    }

    # Multiple applications, force two
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', {
        test_application_name, dest_application_name,
    })
    queue = long_poll(custom_payload={'service': {
        test_application_name: ['stable'],
        dest_application_name: ['stable'],
    }}, current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service'] == {
        dest_application_name: {
            'stable': {'192.168.0.1_1000': j('stable'), '233': j('stable')},
            'direct': {'192.168.0.1_1000': j('stable'), '233': j('stable')},
        },
        test_application_name: {
            'stable': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
            'direct': {'169.254.0.1_1000': j('stable'), '233': j('stable')},
        },
    }

    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', set())


@mark.parametrize('hijack_mode', ['unknown', 'D', 'C', 'E', 'S'])
@mark.xparametrize
def test_enable_force_cluster_route(
        mocker, zk, hijack_context, test_application_name,
        dest_application_name, long_poll, make_cluster,
        test_application, hijack_mode, from_cluster_name, route_dest_cluster,
        force_dest_cluster, req_dest_cluster, rule, intent, use_route):
    mocker.patch.object(settings, 'ROUTE_FORCE_ENABLE_DEST_APPS', set())
    mocker.patch.object(settings, 'FORCE_ROUTING_CLUSTERS', rule)

    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS:
            return True
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    mocker.patch.object(settings, 'ROUTE_HIJACK_LIST', {
        test_application_name: hijack_mode})
    link_cluster = req_dest_cluster + '_link'
    # test ignore link
    make_cluster(dest_application_name, force_dest_cluster,
                 '{"link":["%s""]}' % link_cluster,
                 ['force_dest_cluster_key'])
    # test force enable hijiack
    if req_dest_cluster not in ['direct']:
        make_cluster(dest_application_name, req_dest_cluster,
                     '{}', ['req_dest_cluster_key'])
    make_cluster(dest_application_name, link_cluster,
                 '{}', ['link_cluster_key'])
    rm = RouteManagement(
        huskar_client, test_application_name, from_cluster_name)
    make_cluster(dest_application_name, route_dest_cluster,
                 '{}', ['route_dest_cluster_key'])
    # test ignore route
    rm.set_route(dest_application_name, route_dest_cluster, intent=intent)

    def j(cluster_name):
        return {'value': '{}'}

    config_cluster = 'test_config_cluster'
    switch_cluster = 'test_switch_cluster'
    zk.create(
        '/huskar/switch/%s/overall/overall_key' % (
            dest_application_name), '20',
        makepath=True)
    zk.create(
        '/huskar/switch/%s/%s/switch_key' % (
            dest_application_name, switch_cluster), '50',
        makepath=True)
    zk.create(
        '/huskar/config/%s/overall/overall_key' % (
            dest_application_name), '233',
        makepath=True)
    zk.create(
        '/huskar/config/%s/%s/config_key' % (
            dest_application_name, config_cluster), '666',
        makepath=True)
    custom_payload = {
        'service': {
            dest_application_name: [req_dest_cluster],
        },
        'config': {
            dest_application_name: [config_cluster, 'overall'],
        },
        'switch': {
            dest_application_name: [switch_cluster, 'overall'],
        },
    }
    queue = long_poll(custom_payload=custom_payload, use_route=use_route,
                      current_cluster_name=from_cluster_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    # only force servcie cluster
    assert event['body']['service'] == {
        dest_application_name: {
            intent: {'force_dest_cluster_key': j(force_dest_cluster)},
            req_dest_cluster: {
                'force_dest_cluster_key': j(force_dest_cluster)},
        }
    }
    # switch and config don't force
    assert event['body']['switch'] == {
        dest_application_name: {
            'overall': {'overall_key': {'value': '20'}},
            switch_cluster: {'switch_key': {'value': '50'}},
        }
    }
    assert event['body']['config'] == {
        dest_application_name: {
            'overall': {'overall_key': {'value': '233'}},
            config_cluster: {'config_key': {'value': '666'}},
        }
    }

    # ignore route, link, req clusters updated
    if req_dest_cluster not in ['direct']:
        make_cluster(dest_application_name, req_dest_cluster,
                     '{}', ['req_dest_cluster_key2'])
    make_cluster(dest_application_name, link_cluster,
                 '{}', ['link_cluster_key2'])
    make_cluster(dest_application_name, route_dest_cluster,
                 '{}', ['route_dest_cluster_key2'])
    rm.set_route(dest_application_name, link_cluster, intent=intent)
    gevent.sleep(5)
    assert queue.empty()

    # receive forced cluster update
    make_cluster(dest_application_name, force_dest_cluster,
                 '{"link":["%s""]}' % link_cluster,
                 ['force_dest_cluster_key2'])
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {
        'service': {
            dest_application_name: {
                intent: {'force_dest_cluster_key2': j(force_dest_cluster)},
                req_dest_cluster: {
                    'force_dest_cluster_key2': j(force_dest_cluster)},
            },
        }
    }
    zk.delete('/huskar/service/%s/%s/%s' % (dest_application_name,
              force_dest_cluster, 'force_dest_cluster_key2'), recursive=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'delete'
    assert event['body'] == {
        'service': {
            dest_application_name: {
                intent: {'force_dest_cluster_key2': {'value': None}},
                req_dest_cluster: {'force_dest_cluster_key2': {'value': None}},
            },
        },
    }

    # config and switch update
    zk.create(
        '/huskar/switch/%s/overall/overall_key2' % (
            dest_application_name), '22',
        makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {
        'switch': {
            dest_application_name: {
                'overall': {'overall_key2': {'value': '22'}},
            },
        },
    }
    zk.create(
        '/huskar/config/%s/overall/overall_key2' % (
            dest_application_name), '233_2',
        makepath=True)
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body'] == {
        'config': {
            dest_application_name: {
                'overall': {'overall_key2': {'value': '233_2'}},
            },
        }
    }
    zk.delete('/huskar/switch/%s/overall/overall_key2' % dest_application_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'delete'
    assert event['body'] == {
        'switch': {
            dest_application_name: {
                'overall': {'overall_key2': {'value': None}},
            },
        },
    }
    zk.delete('/huskar/config/%s/overall/overall_key2' % dest_application_name)
    event = queue.get(timeout=5)
    assert event['message'] == 'delete'
    assert event['body'] == {
        'config': {
            dest_application_name: {
                'overall': {'overall_key2': {'value': None}},
            },
        }
    }


def test_hijack_route_hot_config(zk):
    def notify(value):
        watchers = settings.config_manager.external_watchers
        for watcher in watchers['ROUTE_HIJACK_LIST']:
            watcher(value)

    try:
        notify({'foobar': 'X'})
        assert settings.ROUTE_HIJACK_LIST == {'foobar': 'X'}

        notify(None)
        assert settings.ROUTE_HIJACK_LIST == {}
    finally:
        notify(None)


def test_get_service_info(zk, test_application_name, long_poll):
    zk.ensure_path('/huskar/service/%s' % test_application_name)
    zk.ensure_path('/huskar/service/%s/stable' % test_application_name)
    zk.set('/huskar/service/%s' % test_application_name, json.dumps({
        'info': {
          'balance_policy': 'RoundRobin',
          'protocol': 'TCP'
        }
    }))
    zk.set('/huskar/service/%s/stable' % test_application_name, json.dumps({
        'info': {
          'balance_policy': 'Random'
        }
    }))

    # all server and cluster info
    queue = long_poll(service_info=['overall', 'stable'])
    event = queue.get(timeout=5)
    assert event['message'] == 'all'
    assert event['body']['service_info'] == {
        test_application_name: {
            'overall': {
                'balance_policy': {'value': '"RoundRobin"'},
                'protocol': {'value': '"TCP"'}
            },
            'stable': {
                'balance_policy': {'value': '"Random"'}
            },
        },
    }

    # update server info
    zk.set('/huskar/service/%s' % test_application_name, json.dumps({
        'info': {
          'balance_policy': 'RoundRobin'
        }
    }))

    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service_info'][test_application_name] == {
        'overall': {
            'balance_policy': {'value': '"RoundRobin"'}
        }
    }
    assert queue.empty()

    # update cluster info
    zk.set('/huskar/service/%s/stable' % test_application_name, json.dumps({
        'info': {
          'balance_policy': 'RoundRobin'
        }
    }))
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service_info'][test_application_name] == {
        'stable': {
            'balance_policy': {'value': '"RoundRobin"'}
        }
    }
    assert queue.empty()
    stable_queue = long_poll(service_info=['stable'])
    event = stable_queue.get(timeout=5)
    assert event['message'] == 'all'

    # ignore overall cluster info
    zk.ensure_path('/huskar/service/%s/overall' % test_application_name)
    zk.set('/huskar/service/%s/overall' % test_application_name, json.dumps({
        'test_port': '8080'
    }))
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service_info'][test_application_name] == {
        'overall': {
            'balance_policy': {'value': '"RoundRobin"'}
        }
    }
    assert stable_queue.empty()

    # clear server info
    zk.set('/huskar/service/%s' % test_application_name, '{}')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service_info'][test_application_name] == {
        'overall': {}
    }
    assert queue.empty()

    # clear cluster info
    zk.set('/huskar/service/%s/stable' % test_application_name, '{}')
    event = queue.get(timeout=5)
    assert event['message'] == 'update'
    assert event['body']['service_info'][test_application_name] == {
        'stable': {}
    }
    assert queue.empty()


def test_get_service_info_with_malformed_data(
        zk, test_application_name, long_poll):
    queue = long_poll(service_info=['overall'])
    event = queue.get(timeout=5)
    assert event['message'] == 'all'

    zk.set('/huskar/service/%s' % test_application_name, json.dumps({
        'info': {
          'balance_policy': 'RoundRobin'
        }
    }))
    assert queue.empty()
    zk.set('/huskar/service/%s' % test_application_name, json.dumps({
        'info': {
          'balance_policy': 'RoundRobin'
        }
    }))
    event = queue.get(timeout=3)
    assert event['message'] == 'update'


@mark.xparametrize
def test_session_with_max_life_span(
        test_application_name, mocker, switch_on,
        test_application_token, client, max_life_span, jitter,
        life_span, timeout, exclude):
    def fake_switch(name, default=True):
        if name == SWITCH_ENABLE_LONG_POLLING_MAX_LIFE_SPAN:
            return switch_on
        return default

    mocker.patch.object(switch, 'is_switched_on', fake_switch)
    mocker.patch.object(settings, 'LONG_POLLING_MAX_LIFE_SPAN', max_life_span)
    mocker.patch.object(settings, 'LONG_POLLING_LIFE_SPAN_JITTER', jitter)
    if exclude:
        mocker.patch.object(
            settings, 'LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE',
            [test_application_name])
    else:
        mocker.patch.object(
            settings, 'LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE', [])

    url = '/api/data/long_poll'
    data = json.dumps({
        'config': {test_application_name: ['foo']},
        'switch': {test_application_name: ['bar']},
        'service': {test_application_name: ['test']},
    })
    headers = {'Authorization': test_application_token}
    r = client.post(
        url, content_type='application/json', data=data,
        query_string={'life_span': life_span}, headers=headers)
    assert r.status_code == 200, r.data

    with gevent.Timeout(timeout):
        for _ in r.response:
            pass


def test_update_route_force_enable_dest_apps():
    dest_apps = {'233', '666', 'test.foo'}
    try:
        assert settings.ROUTE_FORCE_ENABLE_DEST_APPS == set()
        settings.update_route_force_enable_dest_apps(dest_apps)
        assert settings.ROUTE_FORCE_ENABLE_DEST_APPS == dest_apps
    finally:
        settings.ROUTE_FORCE_ENABLE_DEST_APPS = set()


def test_update_route_force_enable_exclude_source():
    source = {'a': ['233'], 'b': ['666'], 'c.*': ['test.foo']}
    try:
        assert settings.ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP == {}
        settings.update_route_force_enable_exclude_source_map(source)
        assert settings.ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP == source
    finally:
        settings.ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP = {}


def test_update_long_polling_max_life_span_exclude():
    excludes = ['foo', 'bar']
    expected = frozenset(excludes)
    try:
        assert settings.LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE == set()
        settings.update_long_polling_max_life_span_exclude(excludes)
        assert settings.LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE == expected
    finally:
        settings.LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE = set()


def test_declare_upstream(
        zk, client, mocker, test_application, test_dest_application,
        test_application_token):
    from_application_name = test_application.application_name
    from_cluster_name = 'altc1-channel-stable-1'
    to_application_name = test_dest_application.application_name
    to_cluster_name = 'altc1-channel-stable-2'

    is_switched_on = True

    def is_switched_on_func(name, default=True):
        if name == SWITCH_ENABLE_DECLARE_UPSTREAM:
            return is_switched_on
        return default

    mocker.patch.object(switch, 'is_switched_on', is_switched_on_func)

    def reset_everything():
        # Initial state
        zk.delete('/huskar/service/%s' % from_application_name, recursive=True)
        zk.delete('/huskar/service/%s' % to_application_name, recursive=True)

        route_management = RouteManagement(
            huskar_client, from_application_name, from_cluster_name)
        assert sorted(route_management.list_route()) == []
        return route_management

    # Declare upstream application
    route_management = reset_everything()
    r = client.post('/api/data/long_poll', data=json.dumps({
        'service': {to_application_name: [to_cluster_name]}}
    ), content_type='application/json', headers={
        'Authorization': test_application_token,
        'X-SOA-Mode': 'prefix',
        'X-Cluster-Name': from_cluster_name,
    })
    assert r.status_code == 200, r.json
    assert sorted(route_management.list_route()) == [
        (to_application_name, 'direct', None),
    ]

    # Declare nothing because of missing data
    route_management = reset_everything()
    r = client.post('/api/data/long_poll', data=json.dumps({
        'service': {to_application_name: [to_cluster_name]}}
    ), content_type='application/json', headers={
        'Authorization': test_application_token,
    })
    assert r.status_code == 200, r.json
    assert sorted(route_management.list_route()) == []

    # Declare nothing because of switched off
    route_management = reset_everything()
    is_switched_on = False
    r = client.post('/api/data/long_poll', data=json.dumps({
        'service': {to_application_name: [to_cluster_name]}}
    ), content_type='application/json', headers={
        'Authorization': test_application_token,
        'X-SOA-Mode': 'prefix',
        'X-Cluster-Name': from_cluster_name,
    })
    assert r.status_code == 200, r.json
    assert sorted(route_management.list_route()) == []
    is_switched_on = True

    # Declare nothing because of exception
    route_management = reset_everything()
    mocker.patch.object(
        ServiceInfo, 'save', side_effect=RuntimeError('oops'), autospec=True)
    r = client.post('/api/data/long_poll', data=json.dumps({
        'service': {to_application_name: [to_cluster_name]}}
    ), content_type='application/json', headers={
        'Authorization': test_application_token,
        'X-SOA-Mode': 'prefix',
        'X-Cluster-Name': from_cluster_name,
    })
    assert r.status_code == 200, r.json
    assert sorted(route_management.list_route()) == []
    mocker.patch._patches.pop().stop()  # a bit of tricky

    # Declare upstream application twice but save once
    route_management = reset_everything()
    for _ in range(2):
        r = client.post('/api/data/long_poll', data=json.dumps({
            'service': {to_application_name: [to_cluster_name]}}
        ), content_type='application/json', headers={
            'Authorization': test_application_token,
            'X-SOA-Mode': 'prefix',
            'X-Cluster-Name': from_cluster_name,
        })
        assert r.status_code == 200, r.json
    stat = zk.exists('/huskar/service/%s' % from_application_name)
    assert stat.version == 0, 'should save once only'
    assert sorted(route_management.list_route()) == [
        (to_application_name, 'direct', None),
    ]
