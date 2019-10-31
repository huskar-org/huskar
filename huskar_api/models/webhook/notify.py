from __future__ import absolute_import

import logging
import functools
import time
import hashlib
from contextlib import contextmanager

from gevent import spawn
from gevent.queue import Queue, Empty
from gevent.event import Event
from requests import Timeout, ConnectionError, HTTPError
from requests.adapters import HTTPAdapter
from requests import Session

from huskar_api.models.auth import Application
from huskar_api.models.signals import new_action_detected
from huskar_api.models.audit import action_types
from huskar_api.models.audit.const import SEVERITY_DANGEROUS, APPLICATION_USER
from huskar_api.models.const import USER_AGENT
from huskar_api.switch import switch, SWITCH_ENABLE_WEBHOOK_NOTIFY
from huskar_api.extras.monitor import monitor_client
from huskar_api.extras.raven import capture_exception
from .webhook import Webhook

logger = logging.getLogger(__name__)
session = Session()
adapter = HTTPAdapter(max_retries=3)
session.mount('http', adapter)
session.mount('https', adapter)

DEFAULT_HEAERS = {
    'Content-Type': 'application/json',
    'User-Agent': USER_AGENT
}
DEFAULT_TIMEOUT = 2


@contextmanager
def remote_context(url):
    start_at = time.time()
    url_digest = hashlib.md5(url).hexdigest()
    try:
        yield
    except (Timeout, ConnectionError, HTTPError) as e:
        monitor_client.increment('webhook.delivery_errors')
        logger.warn('Remote Request Failed: %s, %s', url_digest, str(e))
    else:
        logger.info('Remote Request Ok: %s', url_digest)
    finally:
        monitor_client.timing('webhook.delivery', time.time() - start_at)


def notify_callback(url, data, headers=None, timeout=None):
    with remote_context(url):
        r = session.post(
            url=url,
            json=data,
            timeout=timeout or DEFAULT_TIMEOUT,
            headers=headers or DEFAULT_HEAERS,
        )
        r.raise_for_status()
        return r


class Notifier(object):

    MAXSIZE = 1000

    def __init__(self, maxsize=None):
        self.hook_queue = Queue(maxsize or self.MAXSIZE)
        self._running = False
        self._empty = Event()
        self._empty.set()

    def _add(self, func, *args, **kwargs):
        if not switch.is_switched_on(SWITCH_ENABLE_WEBHOOK_NOTIFY, True):
            return
        sender = functools.partial(func, *args, **kwargs)
        self.hook_queue.put(sender)

    def start(self):
        if self._running:
            return

        self._running = True
        self._worker = self._spawn_worker()

    def _spawn_worker(self):
        def worker():
            while self._running:
                try:
                    func = self.hook_queue.get(timeout=1)
                    self._empty.clear()
                    try:
                        func()
                    except Exception:
                        logger.exception('Webhook task unexpected failed.')
                        capture_exception(data=None)
                except Empty:
                    self._empty.set()
                    continue
        return spawn(worker)

    def publish(self, application_names, user_name, user_type,
                action_type, action_data=None, severity=SEVERITY_DANGEROUS):
        for application_name in application_names:
            action_name = action_types[action_type]
            application = Application.get_by_name(application_name)
            if application is not None:
                subs = Webhook.search_subscriptions(
                    application_id=application.id,
                    action_type=action_type)
                event_data = {
                    'application_name': application_name,
                    'user_name': user_name,
                    'user_type': user_type,
                    'severity': severity,
                    'action_name': action_name,
                    'action_data': action_data
                }
                for sub in subs:
                    self._add(notify_callback, url=sub.webhook.url,
                              data=event_data)

    def publish_universal(
            self, action_type, username, user_type, action_data, severity):
        action_name = action_types[action_type]
        application_names = []
        if 'application_name' in action_data:
            application_names = [action_data['application_name']]
        if 'application_names' in action_data:
            application_names = action_data['application_names'] or []
        event_data = {
            'application_names': application_names,
            'username': username,
            'user_type': user_type,
            'severity': severity,
            'action_name': action_name,
            'action_data': action_data
        }
        webhooks = Webhook.get_all_universal()
        for webhook in webhooks:
            self._add(notify_callback, url=webhook.url, data=event_data)


notifier = Notifier()


@new_action_detected.connect
def publish_to_subscribers(sender, action_type, username, user_type,
                           action_data, is_subscriable, severity):
    action_name = action_types[action_type]
    logger.info('webhook event(%s, %s) published.',
                action_name, username)
    if user_type == APPLICATION_USER:
        monitor_client.increment('webhook.publish_universal', tags={
            'username': username,
            'appid': username,
            'action_name': action_name,
        })
    notifier.publish_universal(
        action_type, username, user_type, action_data, severity)
    if is_subscriable:
        application_names = (action_data.get('application_names') or
                             [action_data['application_name']])
        for application_name in application_names:
            if user_type == APPLICATION_USER:
                monitor_client.increment('webhook.publish_subscribable', tags={
                    'username': username,
                    'appid': username,
                    'action_name': action_name,
                    'application_name': application_name,
                })
        notifier.publish(application_names, username, user_type,
                         action_type, action_data, severity=severity)
