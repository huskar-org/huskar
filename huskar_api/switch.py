from __future__ import absolute_import

from huskar_api.settings import bootstrap

switch = bootstrap.get_huskar_switch()

SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS = 'switch_enable_route_force_clusters'
SWITCH_ENABLE_MINIMAL_MODE = 'enable_minimal_mode'
SWITCH_ENABLE_AUDIT_LOG = 'enable_audit_log'
SWITCH_ENABLE_SENTRY_MESSAGE = 'enable_sentry_message'
SWITCH_ENABLE_SENTRY_EXCEPTION = 'enable_sentry_exception'
SWITCH_VALIDATE_SCHEMA = 'validate_schema'
SWITCH_ENABLE_WEBHOOK_NOTIFY = 'enable_webhook_notify'
SWITCH_ENABLE_ROUTE_HIJACK = 'enable_route_hijack'
SWITCH_ENABLE_DECLARE_UPSTREAM = 'enable_declare_upstream'
SWITCH_DETECT_BAD_ROUTE = 'detect_bad_route'
SWITCH_ENABLE_EMAIL = 'enable_email'
SWITCH_ENABLE_CONFIG_PREFIX_BLACKLIST = 'enable_config_prefix_blacklist'
SWITCH_ENABLE_META_MESSAGE_CANARY = 'enable_meta_message_canary'
SWITCH_ENABLE_LONG_POLLING_MAX_LIFE_SPAN = 'enable_long_polling_max_life_span'
SWITCH_ENABLE_RATE_LIMITER = 'enable_rate_limiter'
SWITCH_ENABLE_CONCURRENT_LIMITER = 'enable_concurrent_limiter'
SWITCH_ENABLE_ROUTE_HIJACK_WITH_LOCAL_EZONE = (
    'enable_route_hijack_with_local_ezone')
SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN = 'enable_tree_holder_cleaner_clean'
SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK = (
    'enable_tree_holder_cleaner_track')
SWITCH_ENABLE_CONFIG_AND_SWITCH_WRITE = 'enable_config_and_switch_write'
SWITCH_DISABLE_FETCH_VIA_API = 'disable_fetch_via_api'
SWITCH_DISABLE_UPDATE_VIA_API = 'disable_update_via_api'
