from __future__ import absolute_import, print_function

from freezegun import freeze_time

from huskar_api.extras.uptime import process_uptime


def test_process_uptime():
    uptime = process_uptime()
    assert uptime >= 0
    with freeze_time() as frozen_datetime:
        assert int(process_uptime()) == int(uptime)
        frozen_datetime.tick()
        assert int(process_uptime()) == int(uptime) + 1
