from __future__ import absolute_import

import atexit
import logging

from huskar_sdk_v2.bootstrap import BootstrapHuskar

from .consts import HUSKAR_CACHE_DIR

logger = logging.getLogger(__file__)

huskar_client = None


class Bootstrap(object):
    """
    :arg str service: service name of **your** service(e.g. `arch.huskar_api`)
    :arg str servers: Comma-separated list of zookeeper hosts to connect to.
                      (e.g. `127.0.0.1:2181,127.0.0.1:2182`)
    :arg str username: username to connect to zookeeper.
    :arg str password: password to connect to zookeeper.
    :arg str cluster: the cluster name of **your** server(e.g.
                      `alta1-channel-stable-1`),
                      if you don't have the need of multiple cluster(or you
                      just don't understand), leave this default(do not pass).
                      You can named your cluster whatever you like.
                      **BUT DO NOT** use ``overall``, this is a reserved
                      cluster whose configuration will become the default for
                      other clusters, you can override them in the specific
                      cluster. This works just like a base class for other
                      clusters.
    :arg str cache_dir: a path to store cache files, will be used when
                        connection issue occurs to huskar server.
    :arg bool lazy: indicates if :meth:`.start` should be invoked automatically
                    when some methods are called(e.g. `config.get()`), usually
                    you just leave this alone.
    """
    def __init__(self, service, servers='127.0.0.1:2181',
                 username=None, password=None,
                 cluster='overall', cache_dir=HUSKAR_CACHE_DIR,
                 lazy=True):
        self._huskar_client = self._get_huskar_client(
            service, servers=servers, username=username,
            password=password, cluster=cluster, cache_dir=cache_dir,
            lazy=lazy)

    def get_huskar_switch(self):
        """Gets the switch manager by the default app name."""
        return self._create_switch()

    def get_config_manager(self):
        return self._huskar_client.config

    def _get_huskar_client(self, service, servers='127.0.0.1:2181',
                           username=None, password=None,
                           cluster='overall', cache_dir=HUSKAR_CACHE_DIR,
                           lazy=True):
        options = {
            'service': service,
            'servers': servers,
            'username': username,
            'password': password,
            'local_mode': False,
            'cluster': cluster,
            'cache_dir': cache_dir,
            'lazy': lazy,
        }
        huskar_client = BootstrapHuskar(**options)

        # Stop Huskar client explicitly before exiting.
        atexit.register(huskar_client.stop)

        # register_huskar_push_tester(huskar_client)
        return huskar_client

    def _create_switch(self, default_state=True):
        self._huskar_client.switch.set_default_state(default_state)
        return self._huskar_client.switch
