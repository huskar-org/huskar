from functools import partial
import os

from decouple import Config, RepositoryShell

from huskar_api.bootstrap import Bootstrap


class RepositoryHuskar(object):
    """The python-decouple integration of zookeeper and huskar."""

    def __init__(self, manager, fallback, fallback_key_prefix=''):
        self._manager = manager
        self._fallback = fallback
        self._fallback_key_prefix = fallback_key_prefix

    def __contains__(self, key):
        fallback_key = self._fallback_key_prefix + key
        return self._manager.exists(key) or (fallback_key in self._fallback)

    def get(self, key):
        fallback_key = self._fallback_key_prefix + key
        if fallback_key not in self._fallback:
            return self._manager.get(key)
        return self._manager.get(key, default=self._fallback.get(fallback_key))


ENV = os.environ.get('HUSKAR_API_ENV', 'dev')
IS_IN_DEV = ENV == 'dev'
APP_COMMIT = os.environ.get('HUSKAR_API_APP_COMMIT', '')
APP_NAME = os.environ.get('HUSKAR_API_APP_NAME', 'arch.huskar_api')
EZONE = os.environ.get('HUSKAR_API_EZONE', '')
CLUSTER = os.environ.get('HUSKAR_API_CLUSTER', 'dev')
_ZK_SERVERS = os.environ.get('HUSKAR_API_ZK_SERVERS', '127.0.0.1:2181')
_ZK_USERNAME = os.environ.get('HUSKAR_API_ZK_USERNAME', '')
_ZK_PASSWORD = os.environ.get('HUSKAR_API_ZK_PASSWORD', '')
bootstrap = Bootstrap(
    service=APP_NAME,
    servers=_ZK_SERVERS,
    username=_ZK_USERNAME,
    password=_ZK_PASSWORD,
    cluster=CLUSTER
)
config_manager = bootstrap.get_config_manager()
config_repository = RepositoryHuskar(
    manager=config_manager,
    fallback=RepositoryShell(),
    fallback_key_prefix='HUSKAR_API_',
)
config = Config(config_repository)


DEBUG = config.get('DEBUG', cast=bool, default=False)
TESTING = config.get('TESTING', default=False)
SECRET_KEY = config.get('SECRET_KEY', cast=bytes)
SENTRY_DSN = config.get('SENTRY_DSN', default=None)
FALLBACK_SECRET_KEYS = config.get(
    'FALLBACK_SECRET_KEYS', cast=partial(map, bytes), default=[])
DEFAULT_LOCALE = config.get('DEFAULT_LOCALE', 'zh_CN')
DEFAULT_TIMEZONE = config.get('DEFAULT_TIMEZONE', 'Asia/Shanghai')

DB_SETTINGS = {
    'default': {
        'urls': {
            'master': config.get('DB_URL', default=None),
            'slave': config.get('DB_URL', default=None),
        },
        'max_overflow': config.get('DB_MAX_OVERFLOW', default=-1),
        'pool_size': config.get('DB_POOL_SIZE', default=10),
    },
}

ZK_SETTINGS = {
    'username': config.get('ZK_USERNAME', default=_ZK_USERNAME),
    'password': config.get('ZK_PASSWORD', default=_ZK_PASSWORD),
    'servers': config.get('ZK_SERVERS', default=_ZK_SERVERS),
    'start_timeout': config.get('ZK_START_TIMEOUT', default=5),
    'treewatch_timeout': config.get('ZK_TREEWATCH_TIMEOUT', default=5),
}

CACHE_SETTINGS = {
    'default': config.get('REDIS_URL', default=None),
}
CACHE_CONTROL_SETTINGS = config.get('CACHE_CONTROL_SETTINGS', {})

LEGACY_APPLICATION_LIST = frozenset(config.get(
    'LEGACY_APPLICATION_LIST', default=[]
))
CONTAINER_BARRIER_LIFESPAN = config.get('CONTAINER_BARRIER_LIFESPAN', 86400)

ROUTE_IDC_LIST = config.get('ROUTE_IDC_LIST', default=[])      # e.g. 'alta'
ROUTE_EZONE_LIST = config.get('ROUTE_EZONE_LIST', default=[])  # e.g. 'alta1'
ROUTE_INTENT_LIST = config.get('ROUTE_INTENT_LIST', default=['direct'])
ROUTE_DEFAULT_POLICY = config.get('ROUTE_DEFAULT_POLICY', default={
    'direct': 'channel-stable-1',
})
ROUTE_HIJACK_LIST = config.get('ROUTE_HIJACK_LIST', default={})
ROUTE_FROM_CLUSTER_BLACKLIST = config.get(
    'ROUTE_FROM_CLUSTER_BLACKLIST', default={})
ROUTE_DEST_CLUSTER_BLACKLIST = config.get(
    'ROUTE_DEST_CLUSTER_BLACKLIST', default={})
ROUTE_EZONE_DEFAULT_HIJACK_MODE = config.get(
    'ROUTE_EZONE_DEFAULT_HIJACK_MODE', default={})

FORCE_ROUTING_CLUSTERS = config.get('FORCE_ROUTING_CLUSTERS', default={})
ROUTE_FORCE_ENABLE_DEST_APPS = frozenset(config.get(
    'ROUTE_FORCE_ENABLE_DEST_APPS', default=[]))
ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP = config.get(
    'ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP', default={})
ROUTE_EZONE_CLUSTER_MAP = config.get(
    'ROUTE_EZONE_CLUSTER_MAP', default={})
ROUTE_DOMAIN_EZONE_MAP = config.get('ROUTE_DOMAIN_EZONE_MAP', default={})
ROUTE_OVERALL_EZONE = config.get('ROUTE_OVERALL_EZONE', default='')

AUTH_PUBLIC_DOMAIN = config.get('AUTH_PUBLIC_DOMAIN', default=['public'])
AUTH_IP_BLACKLIST = frozenset(config.get('AUTH_IP_BLACKLIST', default=[]))
AUTH_APPLICATION_BLACKLIST = frozenset(
    config.get('AUTH_APPLICATION_BLACKLIST', default=[]))
AUTH_SPREAD_WHITELIST = frozenset(
    config.get('AUTH_SPREAD_WHITELIST', default=[]))

MM_GRACEFUL_STARTUP_TIME = config.get('MINIMAL_MODE_GRACEFUL_STARTUP_TIME', 0)
MM_MIN_RECOVERY_TIME = config.get('MINIMAL_MODE_MIN_RECOVERY_TIME', 20)
MM_MAX_RECOVERY_TIME = config.get('MINIMAL_MODE_MAX_RECOVERY_TIME', 120)
MM_THRESHOLD_DB_ERROR = config.get(
    'MINIMAL_MODE_THRESHOLD_DB_ERROR', default=0.05)
MM_THRESHOLD_UNKNOWN_ERROR = config.get(
    'MINIMAL_MODE_THRESHOLD_UNKNOWN_ERROR', default=0.2)

ADMIN_EMERGENCY_USER_LIST = frozenset(
    config.get('ADMIN_EMERGENCY_USER_LIST', default=[]))
ADMIN_HOME_URL = config.get('ADMIN_HOME_URL', default='')
ADMIN_SIGNUP_URL = config.get('ADMIN_SIGNUP_URL', default='')
ADMIN_RESET_PASSWORD_URL = config.get('ADMIN_RESET_PASSWORD_URL', default='')
ADMIN_SUPPORT_URL = config.get(
    'ADMIN_SUPPORT_URL', default='mailto:huskar@example.com')
ADMIN_INFRA_CONFIG_URL = config.get('ADMIN_INFRA_CONFIG_URL', '')
ADMIN_MAX_EXPIRATION = config.get('ADMIN_MAX_EXPIRATION', default=86400 * 30)
ADMIN_FRONTEND_NAME = config.get(
    'ADMIN_FRONTEND_NAME', default='arch.huskar_fe')
ADMIN_INFRA_OWNER_EMAILS = config.get(
    'ADMIN_INFRA_OWNER_EMAILS', default={})


LONG_POLLING_MAX_LIFE_SPAN = config.get(
    'LONG_POLLING_MAX_LIFE_SPAN', default=3600)
LONG_POLLING_LIFE_SPAN_JITTER = config.get(
    'LONG_POLLING_LIFE_SPAN_JITTER', default=120)
LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE = frozenset(config.get(
    'LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE', default=[]))
TREE_HOLDER_STARTUP_MAX_CONCURRENCY = config.get(
    'TREE_HOLDER_STARTUP_MAX_CONCURRENCY', default=50)
TREE_HOLDER_CLEANER_OLD_OFFSET = config.get(
    'TREE_HOLDER_CLEANER_OLD_OFFSET', default=7)  # 7 days
TREE_HOLDER_CLEANER_CONDITION = config.get(
    'TREE_HOLDER_CLEANER_CONDITION', default='')
TREE_HOLDER_CLEANER_PERIOD = config.get(
    'TREE_HOLDER_CLEANER_PERIOD', default=600)

CONFIG_PREFIX_BLACKLIST = config.get(
    'CONFIG_PREFIX_BLACKLIST', default=['FX_'])

RATE_LIMITER_SETTINGS = config.get('RATE_LIMITER_SETTINGS', default={})
CONCURRENT_LIMITER_SETTINGS = config.get(
    'CONCURRENT_LIMITER_SETTINGS', default={})

TABLE_CACHE_EXPIRATION_TIME = config.get(
    'TABLE_CACHE_EXPIRATION_TIME', default=60 * 10)

FRAMEWORK_VERSIONS = config.get('FRAMEWORK_VERSIONS', default={})
DANGEROUS_ACTION_NAMES_EXCLUDE_LIST = frozenset(config.get(
    'DANGEROUS_ACTION_NAMES_EXCLUDE_LIST', default=[
        'CHANGE_USER_PASSWORD',
        'FORGOT_USER_PASSWORD',
        'OBTAIN_USER_TOKEN',
        'OBTAIN_APPLICATION_TOKEN',
        'CREATE_TEAM',
        'CREATE_USER',
        'CREATE_APPLICATION',
    ]))
APPLICATION_USE_USER_TOKEN_USER_LIST = frozenset(config.get(
    'APPLICATION_USE_USER_TOKEN_USER_LIST', default=[]))

LOCAL_REMOTE_ADDR = config.get('HUSKAR_LOCAL_REMOTE_ADDR', default='')

CONFIG_AND_SWITCH_READONLY_WHITELIST = config.get(
    'CONFIG_AND_SWITCH_READONLY_WHITELIST', default=[])
CONFIG_AND_SWITCH_READONLY_BLACKLIST = config.get(
    'CONFIG_AND_SWITCH_READONLY_BLACKLIST', default=[])
HUSKAR_APPID = 'arch.huskar_api'

must_allow_all_via_api_endpoints = {
    'fetch': frozenset([
        'api.service_weight',
        'api.health_check',
        'api.application_token',
    ]),
    'update': frozenset([
        'api.service',
        # 'api.service_weight',
        # 'api.team',
        'api.team_application_token',
        'api.application',
        'api.application_token',
        'api.service_registry',
        'api.long_polling',
        'api.internal_container_registry',
    ]),
}
ALLOW_ALL_VIA_API_ENDPOINTS = config.get(
    'ALLOW_ALL_VIA_API_ENDPOINTS',
    default=must_allow_all_via_api_endpoints)
# {'api.service': ['foo.test', 'foobar']}
ALLOW_FETCH_VIA_API_USERS = config.get(
    'ALLOW_FETCH_VIA_API_USERS', default={})
# {'api.service': ['foo.test', 'foobar']}
ALLOW_UPDATE_VIA_API_USERS = config.get(
    'ALLOW_UPDATE_VIA_API_USERS', default={})


@config_manager.on_change('CONFIG_AND_SWITCH_READONLY_WHITELIST')
def update_config_and_switch_readonly_whitelist(value):
    global CONFIG_AND_SWITCH_READONLY_WHITELIST
    value = value or []
    if HUSKAR_APPID not in value:
        value .append(HUSKAR_APPID)
    CONFIG_AND_SWITCH_READONLY_WHITELIST = frozenset(value)


@config_manager.on_change('CONFIG_AND_SWITCH_READONLY_BLACKLIST')
def update_config_and_switch_readonly_blacklist(value):
    global CONFIG_AND_SWITCH_READONLY_BLACKLIST
    CONFIG_AND_SWITCH_READONLY_BLACKLIST = frozenset(value or [])


@config_manager.on_change('AUTH_IP_BLACKLIST')
def update_auth_ip_blacklist(value):
    global AUTH_IP_BLACKLIST
    AUTH_IP_BLACKLIST = frozenset(value or [])


@config_manager.on_change('AUTH_APPLICATION_BLACKLIST')
def update_application_blacklist(value):
    global AUTH_APPLICATION_BLACKLIST
    AUTH_APPLICATION_BLACKLIST = frozenset(value or [])


@config_manager.on_change('ROUTE_HIJACK_LIST')
def update_route_hijack_list(value):
    global ROUTE_HIJACK_LIST
    ROUTE_HIJACK_LIST = dict(value or {})


@config_manager.on_change('LEGACY_APPLICATION_LIST')
def update_legacy_application_list(value):
    global LEGACY_APPLICATION_LIST
    LEGACY_APPLICATION_LIST = frozenset(value or [])


@config_manager.on_change('ROUTE_FROM_CLUSTER_BLACKLIST')
def update_route_from_cluster_blacklist(value):
    global ROUTE_FROM_CLUSTER_BLACKLIST
    ROUTE_FROM_CLUSTER_BLACKLIST = dict(value or {})


@config_manager.on_change("FORCE_ROUTING_CLUSTERS")
def update_force_routing_clusters(value):
    global FORCE_ROUTING_CLUSTERS
    FORCE_ROUTING_CLUSTERS = dict(value or {})


@config_manager.on_change('ROUTE_DEST_CLUSTER_BLACKLIST')
def update_route_dest_cluster_blacklist(value):
    global ROUTE_DEST_CLUSTER_BLACKLIST
    ROUTE_DEST_CLUSTER_BLACKLIST = dict(value or {})


@config_manager.on_change('ROUTE_FORCE_ENABLE_DEST_APPS')
def update_route_force_enable_dest_apps(value):
    global ROUTE_FORCE_ENABLE_DEST_APPS
    ROUTE_FORCE_ENABLE_DEST_APPS = frozenset(value or [])


@config_manager.on_change('ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP')
def update_route_force_enable_exclude_source_map(value):
    global ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP
    ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP = dict(value or {})


@config_manager.on_change('RATE_LIMITER_SETTINGS')
def update_rate_limiter_settings(value):
    global RATE_LIMITER_SETTINGS
    RATE_LIMITER_SETTINGS = dict(value or {})


@config_manager.on_change('CONCURRENT_LIMITER_SETTINGS')
def update_concurrent_limiter_settings(value):
    global CONCURRENT_LIMITER_SETTINGS
    CONCURRENT_LIMITER_SETTINGS = dict(value or {})


@config_manager.on_change('FRAMEWORK_VERSIONS')
def update_framework_versions(value):
    global FRAMEWORK_VERSIONS
    FRAMEWORK_VERSIONS = dict(value or {})


@config_manager.on_change('DANGEROUS_ACTION_NAMES_EXCLUDE_LIST')
def update_dangerous_action_names_exclude_list(value):
    global DANGEROUS_ACTION_NAMES_EXCLUDE_LIST
    DANGEROUS_ACTION_NAMES_EXCLUDE_LIST = frozenset(value or [])


@config_manager.on_change('APPLICATION_USE_USER_TOKEN_USER_LIST')
def update_application_use_user_token_user_list(value):
    global APPLICATION_USE_USER_TOKEN_USER_LIST
    APPLICATION_USE_USER_TOKEN_USER_LIST = frozenset(value or [])


@config_manager.on_change('LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE')
def update_long_polling_max_life_span_exclude(value):
    global LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE
    LONG_POLLING_MAX_LIFE_SPAN_EXCLUDE = frozenset(value or [])


@config_manager.on_change('ADMIN_EMERGENCY_USER_LIST')
def update_admin_emergency_user_list(value):
    global ADMIN_EMERGENCY_USER_LIST
    ADMIN_EMERGENCY_USER_LIST = frozenset(value)


@config_manager.on_change('TREE_HOLDER_CLEANER_CONDITION')
def update_tree_holder_cleaner_condition(value):
    global TREE_HOLDER_CLEANER_CONDITION
    TREE_HOLDER_CLEANER_CONDITION = value


@config_manager.on_change('ALLOW_ALL_VIA_API_ENDPOINTS')
def update_allow_all_via_api_endpoints(value):
    global ALLOW_ALL_VIA_API_ENDPOINTS
    final = dict(value or {'fetch': frozenset(), 'update': frozenset()})
    final['fetch'] = frozenset(
        must_allow_all_via_api_endpoints['fetch'] |
        set(final.get('fetch', [])))
    final['update'] = frozenset(
        must_allow_all_via_api_endpoints['update'] |
        set(final.get('update', [])))
    ALLOW_ALL_VIA_API_ENDPOINTS = final


@config_manager.on_change('ALLOW_FETCH_VIA_API_USERS')
def update_allow_fetch_via_api_users(value):
    global ALLOW_FETCH_VIA_API_USERS
    ALLOW_FETCH_VIA_API_USERS = dict(value or {})


@config_manager.on_change('ALLOW_UPDATE_VIA_API_USERS')
def update_allow_update_via_api_users(value):
    global ALLOW_UPDATE_VIA_API_USERS
    ALLOW_UPDATE_VIA_API_USERS = dict(value or {})
