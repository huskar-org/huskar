from __future__ import absolute_import

import logging
import sys

import os
import signal
import gevent

from huskar_api.contrib.backdoor import ServeBackdoor

logger = logging.getLogger(__name__)


def handle_sig(signum, handler):
    signal.signal(signum, handler)
    logger.info('handling signal: %s', signum)


def ignore_sig(signum):
    signal.signal(signum, signal.SIG_IGN)
    logger.info('ignoring %s', signum)


def _start_backdoor_server(signum, frame):
    host = os.environ.get('HUSKAR_API_BS_HOST', '127.0.0.1')
    port = os.environ.get('HUSKAR_API_BS_PORT', 4455)
    addr = (host, int(port))

    try:
        server = ServeBackdoor(addr)
        server.start()
    except: # noqa
        e = sys.exc_info()[1]
        logger.info('failed to start backdoor server on %r: %s', addr, e)
        return

    _handle_ttou()


def _stop_backdoor_server(signum, frame):
    def _stop():
        server = ServeBackdoor.get_instance()
        if server:
            logger.info('stopping backdoor server on %r...', server.addr)
            server.kill()

    ignore_sig(signal.SIGTTOU)
    gevent.spawn(_stop())


def handle_ttin():
    handle_sig(signal.SIGTTIN, _start_backdoor_server)


def _handle_ttou():
    handle_sig(signal.SIGTTIN, _stop_backdoor_server)
