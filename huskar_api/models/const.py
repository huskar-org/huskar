from __future__ import absolute_import

from huskar_api import settings


ENV_DEV = 'dev'

USER_AGENT = u'%s/%s' % (
    settings.APP_NAME, settings.APP_COMMIT[:8] if settings.APP_COMMIT
    else u'unknown-version')

ROUTE_DEFAULT_INTENT = 'direct'
ROUTE_MODE_ROUTE = 'route'
ROUTE_MODES = ('orig', 'prefix', ROUTE_MODE_ROUTE)
ROUTE_LINKS_DELIMITER = u'+'

MM_REASON_AUTH = 'auth'
MM_REASON_SWITCH = 'switch'
MM_REASON_TESTER = 'tester'
MM_REASON_STARTUP = 'startup'

SELF_APPLICATION_NAME = settings.APP_NAME

# Spec http://example.com/design/infra_key.html
INFRA_CONFIG_KEYS = {
    'database': 'FX_DATABASE_SETTINGS',  # Final
    'redis': 'FX_REDIS_SETTINGS',        # Final
    'amqp': 'FX_AMQP_SETTINGS',          # Final
    'es': 'FX_ES_SETTINGS',              # Draft
    'mongo': 'FX_MONGO_SETTINGS',        # Draft
    'oss': 'FX_OSS_SETTINGS',            # Draft
    'kafka': 'FX_KAFKA_SETTINGS',        # Draft
}

# scope types
SCOPE_SITE = 0
SCOPE_TEAM = 1
SCOPE_APPLICATION = 2
SCOPE_SCENE = 3

EXTRA_SUBDOMAIN_SERVICE_INFO = 'service_info'

MAGIC_CONFIG_KEYS = {
    'batch_config.inclusive_keys': 'HUSKAR_BATCH_CONFIG_INCLUSIVE_KEYS',
}
RELEASE_WINDOW_BYPASS_VALUE = 'bypass'
