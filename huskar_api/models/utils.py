from __future__ import absolute_import

import copy
import functools
import itertools
import inspect
import json
import re

from huskar_sdk_v2.consts import OVERALL
from more_itertools import peekable, first
from dogpile.cache.util import function_key_generator
from gevent import sleep

from huskar_api import settings
from huskar_api.models.const import MAGIC_CONFIG_KEYS
from huskar_api.models.cache.region import zdumps, zloads
from huskar_api.extras.raven import capture_exception


__all__ = [
    'make_cache_decorator',
    'take_slice',
    'check_znode_path',
    'dedupleft',
    'merge_instance_list',
]


def retry(exceptions, interval, max_retry):
    """A decorator with arguments to make view functions could be retried.

    :param exceptions: The tuple of exception set.
    :param interval: The sleep interval in seconds.
    :param max_retry: The max retry times.
    """
    def decorator(wrapped):
        @functools.wraps(wrapped)
        def wrapper(*args, **kwargs):
            for i in xrange(max_retry):
                try:
                    return wrapped(*args, **kwargs)
                except exceptions:
                    if i == max_retry - 1:
                        raise
                    sleep(interval)
        return wrapper
    return decorator


def make_cache_decorator(redis_client):
    """Creates a decorator to apply cache on arguments."""

    def cache_on_arguments(expiration_time):
        def decorator(fn):
            fn_generate_key = function_key_generator(
                'cache_on_arguments:v1', fn, to_str=unicode)
            fn_args = inspect.getargspec(fn)
            fn_has_self = fn_args[0] and fn_args[0][0] in ('self', 'cls')

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                key = fn_generate_key(*args, **kwargs)
                val = redis_client.get(key)
                if val is None:
                    val = fn(*args, **kwargs)
                    redis_client.set(key, zdumps(val), expiration_time)
                    return val
                else:
                    return zloads(val)

            def generate_key(*args, **kwargs):
                args = ((None,) + args) if fn_has_self else args
                return fn_generate_key(*args, **kwargs)

            def flush(*args, **kwargs):
                return redis_client.delete(generate_key(*args, **kwargs))

            wrapper.generate_key = generate_key
            wrapper.flush = flush
            wrapper.original = fn

            return wrapper
        return decorator
    return cache_on_arguments


class LazySlice(object):
    """Takes the slice of iterable object and passes it into a factory
    function.

    Example::

        @classmethod
        def list_foo(cls):
            ids = cls.get_foo_ids()
            return take_slice(cls.mget, ids)

        assert Spam.list_foo()[:10] == Spam.mget(Spam.get_foo_ids()[:10])
    """

    def __init__(self, factory, iterable):
        self.factory = factory
        self.iterable = iterable

    def __iter__(self):
        return iter(self.factory(list(self.iterable)))

    def __getitem__(self, s):
        assert isinstance(s, slice)
        iterable = itertools.islice(self.iterable, s.start, s.stop, s.step)
        return self.factory(list(iterable))


take_slice = LazySlice


# Reference: https://git.io/zookeeper-3.5.3-validate-path
re_znode_path = re.compile(
    ur'^(?!^\.+$)([^\u0000-\u001F\u007F-\u009F\ud800-\uF8FF\uFFF0-\uFFFF]+)$'
)


def check_znode_path(*components):
    for comp in components:
        if (not comp or comp.strip() != comp or
                any(c in comp for c in '/\n\r\t') or
                re_znode_path.search(comp) is None):
            raise ValueError(
                    'Illegal characters in path({!r})'.format(components))


def normalize_cluster_name(cluster_name):
    """Normalizes the cluster name to avoid from duplicated E-Zone prefix."""
    fragments = cluster_name.split(u'-')
    prefix = first(fragments)
    if prefix and prefix in settings.ROUTE_EZONE_LIST:
        return u'-'.join(dedupleft(fragments, prefix))
    return unicode(cluster_name)


def dedupleft(iterable, marker):
    """Deduplicates the marker on the left of an iterable object."""
    iterator = peekable(iterable)
    for x in iterator:
        if iterator.peek(None) != marker:
            break
    return itertools.chain([marker], iterator)


def merge_instance_list(
        application_name, overall_instance_list, current_instance_list,
        cluster_name):
    new_instance_list = []
    instance_key_index_map = {}
    for index, instance in enumerate(overall_instance_list):
        if instance['cluster'] != OVERALL:
            continue
        key = (application_name, cluster_name, instance['key'])
        instance_key_index_map[key] = index
        new_instance = copy.copy(instance)
        new_instance['cluster'] = cluster_name
        new_instance_list.append(new_instance)

    for instance in current_instance_list:
        key = (application_name, cluster_name, instance['key'])
        if key in instance_key_index_map:
            new_instance_list[instance_key_index_map[key]] = instance
        else:
            instance_key_index_map[key] = len(new_instance_list)
            new_instance_list.append(instance)

    new_instance_list = _process_instance_list(
        application_name, cluster_name, new_instance_list,
        instance_key_index_map)
    return new_instance_list


def _process_instance_list(
        application_name, cluster_name, new_instance_list,
        instance_key_index_map):
    # Find inclusive keys
    inclusive_keys = frozenset()
    _key = MAGIC_CONFIG_KEYS['batch_config.inclusive_keys']
    key = (application_name, cluster_name, _key)
    if key in instance_key_index_map:
        instance = new_instance_list[instance_key_index_map[key]]
        try:
            inclusive_keys = frozenset(json.loads(instance['value']))
        except (KeyError, ValueError, TypeError):
            capture_exception()

    # Process instance list
    if not inclusive_keys:
        return new_instance_list

    return [i for i in new_instance_list if i['key'] in inclusive_keys]
