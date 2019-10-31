from __future__ import absolute_import

from huskar_sdk_v2.utils import combine
from huskar_sdk_v2.consts import BASE_PATH

from huskar_api.models import huskar_client
from huskar_api.models.znode import ZnodeList


__all__ = ['application_manifest']


class ApplicationManifest(object):
    """The manifest of all applications in ZooKeeper.

    This model serves the minimal mode of Huskar API. Once the database falls
    in a system outage, the API will provide application list here instead.
    """

    def __init__(self, huskar_client):
        self._lists = [
            ZnodeList(huskar_client.client, combine(BASE_PATH, 'service')),
            ZnodeList(huskar_client.client, combine(BASE_PATH, 'switch')),
            ZnodeList(huskar_client.client, combine(BASE_PATH, 'config')),
        ]

    def start(self):
        for l in self._lists:
            l.start()

    def check_is_application(self, name):
        return any(name in l.children for l in self._lists)

    def as_list(self):
        return sorted({c for l in self._lists for c in l.children})


application_manifest = ApplicationManifest(huskar_client)
application_manifest.start()
