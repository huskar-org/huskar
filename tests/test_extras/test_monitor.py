from __future__ import absolute_import

from huskar_api.extras.monitor import MonitorClient


def test_for_cov():
    c = MonitorClient()
    assert c.increment('test') is None
    assert c.timing('test', 233) is None
    assert c.payload('test') is None
