from __future__ import absolute_import

import logging
import time

from huskar_api.models import redis_client

logger = logging.getLogger(__name__)
SCRIPT = '''
local tokens_key = KEYS[1]
local timestamp_key = KEYS[2]

local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local fill_time = capacity/rate
local ttl = math.floor(fill_time+0.999)

local last_tokens = tonumber(redis.call("get", tokens_key))
if last_tokens == nil then
  last_tokens = capacity
end

local last_refreshed = tonumber(redis.call("get", timestamp_key))
if last_refreshed == nil then
  last_refreshed = 0
end

local delta = math.max(0, now-last_refreshed)
local filled_tokens = math.min(capacity, last_tokens+(delta*rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
if allowed then
  new_tokens = filled_tokens - requested
end

redis.call("setex", tokens_key, ttl, new_tokens)
redis.call("setex", timestamp_key, ttl, now)

return { allowed, new_tokens }
'''
PREFIX = 'huskar_api.request_rate_limiter'


def check_new_request(identity, rate, capacity, requested=1):
    prefix = '{%s}' % ('{}.{}'.format(PREFIX, identity))
    keys = ['{}.tokens'.format(prefix), '{}.timestamp'.format(prefix)]
    keys_and_args = keys + [rate, capacity, int(time.time()), requested]
    try:
        allowed, tokens_left = redis_client.eval(
            SCRIPT, len(keys), *keys_and_args)
    except Exception as e:
        logger.warning('check new request for rate limit error: %s', e)
        return

    if allowed != 1:
        raise RateExceededError()


class RateExceededError(Exception):
    pass
