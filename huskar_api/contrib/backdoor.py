import logging

from gevent import Greenlet
from gevent.backdoor import BackdoorServer

logger = logging.getLogger(__name__)


class ServeBackdoor(Greenlet):
    __instance = None

    def __init__(self, addr, *args, **kwargs):
        if self.__class__.__instance is not None:
            raise RuntimeError(
                'only one backdoor server allowed to be running'
            )
        self.addr = addr
        self.server = BackdoorServer(addr)
        Greenlet.__init__(self, *args, **kwargs)

    @classmethod
    def get_instance(cls):
        return cls.__instance

    # pylint: disable=E0202
    def _run(self):
        cls = self.__class__
        try:
            cls.__instance = self
            logger.info("starting backdoor server on %r...", self.addr)
            self.server.serve_forever()
        finally:
            logger.info('backdoor server on %r stopped', self.addr)
            cls.__instance = None
