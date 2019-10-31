from __future__ import absolute_import

import time


_start_time = time.time()


def process_uptime():
    """Gets the uptime of current process."""
    return time.time() - _start_time
