from __future__ import absolute_import

import logging
import time

import gevent
from gevent.event import Event
import psutil

from huskar_api import settings
from huskar_api.extras.monitor import monitor_client
from huskar_api.extras.raven import capture_exception
from huskar_api.models import redis_client
from huskar_api.switch import (
    switch, SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN,
    SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK)

logger = logging.getLogger(__name__)
REDIS_KEY = 'huskar_api.tree_holder_cleaner'


class TreeHolderCleaner(object):
    def __init__(self, tree_hub):
        self._tree_hub = tree_hub
        self._old_offset = (
            60 * 60 * 24 * settings.TREE_HOLDER_CLEANER_OLD_OFFSET)
        self._period = settings.TREE_HOLDER_CLEANER_PERIOD
        self._stopped = Event()

    def track(self, application_name, type_name):
        if not switch.is_switched_on(
                SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK):
            return

        name = '{}:{}'.format(application_name, type_name)
        score = time.time()
        try:
            redis_client.zadd(REDIS_KEY, **{name: score})
        except Exception as e:
            logger.warning('tree holder cleaner track item failed: %s', e)

    def clean(self):
        if not (
            switch.is_switched_on(
                SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK) and
            switch.is_switched_on(
                SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN, False)):
            return

        if self._is_time_to_clean():
            self._clean()

    def spawn_cleaning_thread(self):
        gevent.spawn(self._worker)

    def _worker(self):
        while not self._stopped.is_set():
            self.clean()
            gevent.sleep(self._period)

    def _clean(self):
        max_score = time.time() - self._old_offset
        try:
            items = redis_client.zrangebyscore(REDIS_KEY, 0, max_score)
        except Exception as e:
            logger.warning('get tree holder cleaner data failed: %s', e)
            return

        for key in items:
            application_name, type_name = key.split(':')
            holder = self._tree_hub.release_tree_holder(
                application_name, type_name)
            if holder is not None:
                logger.info(
                    'release unused tree holder: %s %s', application_name,
                    type_name)
                monitor_client.increment('tree_holder.release_unused', tags={
                    'application_name': application_name,
                    'appid': application_name,
                    'type_name': type_name,
                })

        self._clean_old_redis_data()

    def _clean_old_redis_data(self):
        max_score = time.time() - self._old_offset * 3
        try:
            redis_client.zremrangebyscore(REDIS_KEY, 0, max_score)
        except Exception as e:
            logger.warning('clean tree holder cleaner old data failed: %s', e)

    def _is_time_to_clean(self):
        condition = settings.TREE_HOLDER_CLEANER_CONDITION
        if not condition:
            return False

        cpu = self._get_cpu_percent()
        memory = self._get_virtual_memory_percent()
        # e.g. 'cpu < 50 and memory > 90'
        condition = condition.replace(
            'cpu', str(cpu)).replace('memory', str(memory))
        try:
            return eval(condition, {}, {})
        except BaseException as e:
            logger.error(
                'invalid tree holder cleaner condition: %r %s', condition, e)
            capture_exception('invalid tree holder cleaner condition')
            return False

    def _get_cpu_percent(self):
        return psutil.cpu_percent()

    def _get_virtual_memory_percent(self):
        return psutil.virtual_memory().percent
