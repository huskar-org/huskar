from __future__ import absolute_import

import functools
import itertools

from meepo2.signals import signal
from meepo2.apps.eventsourcing import sqlalchemy_es_pub


def catch_cache_update_exc(func):
    @functools.wraps(func)
    def _(self, data_obj, model, pk_name, pk_value):
        ret = False
        try:
            ret = func(self, data_obj, model, pk_name, pk_value)
            return ret
        finally:
            if not ret:
                model.flush([pk_value])
                # call hooks
                model._call_set_fail_callbacks(data_obj, pk_name, pk_value)
    return _


class EventHook(sqlalchemy_es_pub):
    def __init__(self, cache_clients, session, tables=None):
        super(EventHook, self).__init__(session, tables)

        self.default_log_level = 'WARNING'
        self._setup_log()
        self.cache_clients = cache_clients

    def _setup_log(self):
            self.logger.setLevel(self.default_log_level)

    def add(self, model):
        tablename = model.__tablename__

        self.tables.add(tablename)

        self.install_cache_signal(tablename)

        self.logger.info("cache set hook enabled for table: {}".format(
            tablename))

    def install_cache_signal(self, table):
        rawdata_event = "{}_rawdata".format(table)
        delete_event = "{}_delete_raw".format(table)

        signal(rawdata_event).connect(self._rawdata_sub, weak=False)
        signal(delete_event).connect(self._delete_sub, weak=False)

    def _rawdata_sub(self, raw_obj, model):
        pk_name = model.pk_name()
        tablename = model.__tablename__
        pk = raw_obj[pk_name]

        ret = self._set_rawdata_obj(raw_obj, model, pk_name, pk)

        self.logger.info("set raw data cache for {} {}, state: {}".format(
            tablename, pk, ret))

    @catch_cache_update_exc
    def _set_rawdata_obj(self, data_obj, model, pk_name, pk_value):
        return model.set_raw(data_obj)

    def _delete_sub(self, obj):
        ret = obj.flush([obj.pk])

        self.logger.info("delete cache for {} {}, state: {}".format(
            obj.__tablename__, obj.pk, ret))

    # after flush
    def session_prepare(self, session, _):
        super(EventHook, self).session_prepare(session, _)

        if not getattr(session, "pending_rawdata", None):
            session.pending_rawdata = {}

        for obj in itertools.chain(session.pending_write,
                                   session.pending_update):

            if obj.__tablename__ not in self.tables:
                continue

            key = obj.pk, obj.__tablename__

            session.pending_rawdata[key] = obj.__rawdata__, obj.__class__

    # after commit
    def session_commit(self, session):
        if getattr(session, "pending_rawdata", None):
            self._pub_cache_events("rawdata", session.pending_rawdata)
            del session.pending_rawdata

        super(EventHook, self).session_commit(session)

    # after rollback
    def session_rollback(self, session):
        if getattr(session, "pending_rawdata", None):
            del session.pending_rawdata

        super(EventHook, self).session_rollback(session)

    def _pub_cache_events(self, event_type, objs):
        if not objs:
            return

        for obj, model in objs.values():
            sg_name = "{}_{}".format(model.__tablename__, event_type)
            signal(sg_name).send(obj, model=model)
