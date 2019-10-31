from __future__ import absolute_import

import logging

from marshmallow import ValidationError
from kazoo.exceptions import NoNodeError, NodeExistsError, BadVersionError
from kazoo.protocol.states import EventType

from huskar_api.extras.payload import zk_payload
from .exceptions import MalformedDataError, OutOfSyncError
from .utils import check_znode_path

logger = logging.getLogger(__name__)


class ZnodeModel(object):
    """A data mapper between ZooKeeper and in-memory model.

    This mapper could prevent concurrent writing by optimistic concurrencty
    control.
    """

    PATH_PATTERN = None
    MARSHMALLOW_SCHEMA = None

    _MALFORMED_DATA_EXCEPTIONS = (ValueError, ValidationError)

    def __init__(self, client, **kwargs):
        check_znode_path(*kwargs.values())
        self.client = client
        self.path = self.PATH_PATTERN.format(**kwargs)
        self.data = None
        self.stat = None

    def setdefault(self, value):
        if self.data is None:
            self.data = value
        return self.data

    def load(self):
        """Loads data from ZooKeeper and parses it.

        The :attr:`ZnodeModel.stat` will be ``None`` if the target node does
        not exist.

        :raises MalformedDataError: The data source is malformed.
        """
        try:
            data, stat = self.client.get(self.path)
        except NoNodeError:
            return
        self.stat = stat
        if data:
            try:
                self.data, _ = self.MARSHMALLOW_SCHEMA.loads(data)
            except self._MALFORMED_DATA_EXCEPTIONS as e:
                raise MalformedDataError(self, e)

    def save(self, version=None):
        """Saves the data in this instance to ZooKeeper.

        It is concurrency-safe if you never break the :attr:`ZnodeModel.stat`.

        :param version: Optional. The alternative version instead of
                        :attr:`ZnodeModel.stat`.
        :raises OutOfSyncError: The local data is outdated.
        :raises marshmallow.ValidationError: :attr:`ZnodeModel.data` is invalid
        """
        data, _ = self.MARSHMALLOW_SCHEMA.dumps(self.data)
        self.MARSHMALLOW_SCHEMA.loads(data)  # raise ValidationError if need
        if self.stat is None:
            try:
                self.client.create(self.path, data, makepath=True)
                zk_payload(payload_data=data, payload_type='create')
            except NodeExistsError as e:
                raise OutOfSyncError(e)
            self.load()
        else:
            if version is None:
                version = self.stat.version
            try:
                self.stat = self.client.set(self.path, data, version=version)
                zk_payload(payload_data=data, payload_type='set')
            except (NoNodeError, BadVersionError) as e:
                raise OutOfSyncError(e)

    def delete(self, version=None):
        """Deletes this znode from ZooKeeper.

        It is concurrency-safe if you never break the :attr:`ZnodeModel.stat`.

        :param version: Optional. The alternative version instead of
                        :attr:`ZnodeModel.stat`.
        :raises OutOfSyncError: The model is not loaded yet, or the local data
                                is outdated.
        """
        if self.stat is None:
            raise OutOfSyncError()
        if version is None:
            version = self.stat.version
        try:
            self.client.delete(self.path, version=version, recursive=False)
        except BadVersionError as e:
            raise OutOfSyncError(e)
        else:
            self.stat = None
            self.data = None


class ZnodeList(object):
    """A in-memory and auto-synchronizing list of specific ZooKeeper nodes.

    For now this list is read-only and only mutable by ZooKeeper events.
    """

    def __init__(self, client, parent_path, on_update=None):
        self.client = client
        self.parent_path = parent_path
        self.children = frozenset()
        self.data_watch = None
        self.children_watch = None
        self.started = False
        self.on_update = on_update

    def __repr__(self):
        return 'ZnodeList(%r, %r)' % (self.client, self.parent_path)

    def start(self):
        """Starts the synchronizing and fetches initial data."""
        if self.started:
            return
        self.started = True
        client = self.client
        self.data_watch = client.DataWatch(self.parent_path, self._handle_data)
        self.children_watch = client.ChildrenWatch(
            self.parent_path, self._handle_children)

    def post_update(self):
        if self.on_update is None:
            return
        self.on_update(self.children)

    def _handle_data(self, data, stat, event):
        if event is None:
            return

        if event.type == EventType.CREATED and self.children_watch._stopped:
            self.children_watch._stopped = False
            self.children_watch._watcher(event)
            logger.info('%r was restarted', self)
            return

        if event.type == EventType.DELETED:
            self.children = frozenset([])
            logger.info('%r was cleared', self)
            self.post_update()
            return

    def _handle_children(self, children):
        self.children = frozenset(children)
        logger.info('%r was updated', self)
        self.post_update()
