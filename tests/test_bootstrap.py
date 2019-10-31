from __future__ import absolute_import

from gevent import sleep

from huskar_api.switch import switch


def test_switch_change_on_fly(zk):
    switch_name = 'test_switch_change_on_fly'
    path = '/huskar/switch/arch.huskar_api/overall/%s' % switch_name
    zk.ensure_path(path)
    zk.set(path, '0')
    sleep(1)
    assert switch.is_switched_on(switch_name, default=None) is False

    # update
    zk.set(path, '100')
    sleep(1)
    assert switch.is_switched_on(switch_name, default=None) is True
