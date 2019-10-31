from __future__ import absolute_import

import logging
import time
import uuid

from huskar_api.models import redis_client

logger = logging.getLogger(__name__)
SCRIPT = '''
local key = KEYS[1]

local capacity = tonumber(ARGV[1])
local timestamp = tonumber(ARGV[2])
local id = ARGV[3]

local count = redis.call("zcard", key)
local allowed = count < capacity

if allowed then
  redis.call("zadd", key, timestamp, id)
end

return { allowed, count }
'''
PREFIX = 'huskar_api.concurrent_requests_limiter'


def check_new_request(identity, ttl, capacity):
    timestamp = int(time.time())
    key = '{}.{}'.format(PREFIX, identity)
    sub_item = str(uuid.uuid4())
    keys = [key]
    keys_and_args = keys + [capacity, timestamp, sub_item]
    try:
        redis_client.zremrangebyscore(key, '-inf', timestamp - ttl)
        allowed, count = redis_client.eval(SCRIPT, len(keys), *keys_and_args)
    except Exception as e:
        logger.warning('check new request for concurrent limit error: %s', e)
        return

    if allowed != 1:
        raise ConcurrencyExceededError()

    return key, sub_item


def release_request(key, sub_item):
    try:
        redis_client.zrem(key, sub_item)
    except Exception as e:
        logger.warning('release request for concurrent limit error: %s', e)


def release_after_iterator_end(data, iterator):
    try:
        for item in iterator:
            yield item
    finally:
        if data:
            release_request(data['key'], data['sub_item'])


class ConcurrencyExceededError(Exception):
    pass
