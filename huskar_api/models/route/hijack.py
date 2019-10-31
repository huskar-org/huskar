from __future__ import absolute_import

import copy
import logging

from enum import Enum
from huskar_sdk_v2.consts import SERVICE_SUBDOMAIN

from huskar_api import settings
from huskar_api.switch import (
    switch, SWITCH_ENABLE_ROUTE_HIJACK,
    SWITCH_ENABLE_ROUTE_HIJACK_WITH_LOCAL_EZONE,
    SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS)
from huskar_api.extras.monitor import monitor_client
from huskar_api.extras.raven import capture_message
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.const import (
    ROUTE_DEFAULT_INTENT, ROUTE_MODE_ROUTE)
from .hijack_stage import lookup_route_stage
from .utils import try_to_extract_ezone

logger = logging.getLogger(__name__)


class RouteHijack(object):
    class Mode(Enum):
        disabled = 'D'
        checking = 'C'
        enabled = 'E'
        standalone = 'S'

    def __init__(self, huskar_client, from_application_name, from_cluster_name,
                 remote_addr, route_mode, request_domain):
        self.huskar_client = huskar_client
        self.from_application_name = from_application_name
        self.from_cluster_name = from_cluster_name
        self.remote_addr = remote_addr
        self.route_mode = route_mode
        if switch.is_switched_on(SWITCH_ENABLE_ROUTE_HIJACK):
            ezone = _get_ezone(request_domain, from_application_name,
                               self.from_cluster_name, remote_addr)
            default_hijack_mode = settings.ROUTE_EZONE_DEFAULT_HIJACK_MODE.get(
                ezone, self.Mode.disabled.value)
            route_hijack_list = _get_route_hijack_list(
                from_application_name, ezone)
            try:
                self.hijack_mode = self.Mode(
                    route_hijack_list.get(
                        self.from_application_name, default_hijack_mode))
            except ValueError:
                logger.warning(
                    'Invalid hijack mode: %s', self.from_application_name)
                self.hijack_mode = self.Mode.disabled
        else:
            self.hijack_mode = self.Mode.disabled
        self.hijack_map = {}
        self._force_enable_dest_apps = set()

    def prepare(self, tree_watcher, request_data):
        """Reads data sources."""
        if (self.from_application_name in settings.LEGACY_APPLICATION_LIST or
                not self.from_cluster_name):
            logger.info('Skip: %s %s %s', self.from_application_name,
                        self.remote_addr, self.from_cluster_name)
            self.hijack_mode = self.Mode.disabled

        if (switch.is_switched_on(SWITCH_ENABLE_ROUTE_FORCE_CLUSTERS,
                                  default=False) and
                self.from_cluster_name in settings.FORCE_ROUTING_CLUSTERS):
            self.hijack_mode = self.Mode.standalone

        self._force_enable_dest_apps = set(
            _get_force_enable_dest_apps(
                self.from_application_name, request_data))

        if (self.hijack_mode in (self.Mode.enabled, self.Mode.standalone) or
                self._force_enable_dest_apps):
            tree_watcher.from_application_name = self.from_application_name
            tree_watcher.from_cluster_name = self.from_cluster_name

    def hijack_request(self, request_data):
        """Checks or hijacks the request arguments."""
        if self.route_mode == ROUTE_MODE_ROUTE:
            return request_data

        if (self.hijack_mode is self.Mode.disabled and
                not self._force_enable_dest_apps):
            return request_data

        request_data = copy.deepcopy(request_data)
        for type_name, application_names in request_data.items():
            if type_name != SERVICE_SUBDOMAIN:
                continue
            for application_name, cluster_names in application_names.items():
                intent_map = _build_intent_map(cluster_names)
                self.hijack_map[application_name] = intent_map
                if self.hijack_mode in (self.Mode.checking, self.Mode.enabled):
                    self._check_request(application_name, intent_map)
                if (self.hijack_mode in (
                        self.Mode.enabled, self.Mode.standalone) or
                        application_name in self._force_enable_dest_apps):
                    application_names[application_name] = [
                        intent for intent, icluster_names in intent_map.items()
                        if icluster_names]
                    logger.info(
                        'Hijack: %s %s -> %s %s', self.from_application_name,
                        self.from_cluster_name, application_name, intent_map)
        return request_data

    def hijack_response(self, tree_watcher):
        """Checks or hijacks the response iterator."""
        is_enabled = (
            self.hijack_mode in (self.Mode.enabled, self.Mode.standalone) and
            self.route_mode != ROUTE_MODE_ROUTE)
        is_enabled = is_enabled or len(self._force_enable_dest_apps) > 0
        for message, body in tree_watcher:
            if is_enabled and SERVICE_SUBDOMAIN in body:
                body = copy.deepcopy(body)
                type_body = body[SERVICE_SUBDOMAIN]
                for application_name, intent_map in self.hijack_map.items():
                    if application_name not in type_body:
                        continue
                    application_body = type_body[application_name]
                    for intent, cluster_names in intent_map.items():
                        if intent not in application_body:
                            continue
                        cluster_body = application_body[intent]
                        for cluster_name in cluster_names:
                            application_body[cluster_name] = cluster_body
            yield message, body

    def _check_request(self, application_name, intent_map):
        if application_name in settings.LEGACY_APPLICATION_LIST:
            return

        im = InstanceManagement(
            self.huskar_client, application_name, SERVICE_SUBDOMAIN)
        im.set_context(self.from_application_name, self.from_cluster_name)
        for intent, icluster_names in intent_map.iteritems():
            if not icluster_names:   # pragma: no cover  # TODO: fix
                continue
            dest_cluster_blacklist = settings.ROUTE_DEST_CLUSTER_BLACKLIST.get(
                application_name, [])
            if self.hijack_mode is self.Mode.checking:
                if icluster_names & set(dest_cluster_blacklist):
                    continue

            if len(icluster_names) > 1:
                logger.info(
                    '[%s]Unstable: %s %s -> %s %s', self.hijack_mode.value,
                    self.from_application_name, self.from_cluster_name,
                    application_name, intent_map)
                capture_message(
                    '[%s]RouteHijack unstable' % self.hijack_mode.value,
                    extra={
                        'from_application_name': (
                            self.from_application_name),
                        'from_cluster_name': self.from_cluster_name,
                        'application_name': application_name,
                        'intent_map': repr(intent_map),
                        'intent': intent,
                    })
                continue

            resolved_name = im.resolve_cluster_name(intent)
            cluster_name = list(icluster_names)[0]
            cluster_name = (
                im.resolve_cluster_name(cluster_name) or cluster_name)
            if resolved_name != cluster_name:
                logger.info(
                    '[%s]Mismatch: %s %s -> %s %s %s %s',
                    self.hijack_mode.value, self.from_application_name,
                    self.from_cluster_name, application_name, intent_map,
                    resolved_name, cluster_name)
                if self.hijack_mode is self.Mode.checking:
                    capture_message(
                        '[%s]RouteHijack mismatch' % self.hijack_mode.value,
                        extra={
                            'from_application_name': (
                                self.from_application_name),
                            'from_cluster_name': self.from_cluster_name,
                            'application_name': application_name,
                            'cluster_name': cluster_name,
                            'intent_map': repr(intent_map),
                            'intent': intent,
                            'resolved_name': resolved_name,
                        })
                self.analyse_mismatch(application_name, cluster_name,
                                      resolved_name, intent, intent_map)

    def analyse_mismatch(self, dest_application_name, orig_dest_cluster_name,
                         resolved_dest_cluster_name, intent, intent_map):
        logger.info(
            '[%s]Unexpected mismatch: %s %s -> %s %s %s %s',
            self.hijack_mode.value, self.from_application_name,
            self.from_cluster_name, dest_application_name,
            intent_map, resolved_dest_cluster_name,
            orig_dest_cluster_name)
        capture_message(
            '[%s]Unexpected RouteHijack mismatch' % self.hijack_mode.value,
            extra={
                'from_application_name': self.from_application_name,
                'from_cluster_name': self.from_cluster_name,
                'application_name': dest_application_name,
                'cluster_name': orig_dest_cluster_name,
                'intent_map': repr(intent_map),
                'intent': intent,
                'resolved_name': resolved_dest_cluster_name},
            tags={
                'component': __name__,
                'from_application_name': self.from_application_name,
                'application_name': dest_application_name})


def _build_intent_map(cluster_names):
    r = {ROUTE_DEFAULT_INTENT: set()}
    for cluster_name in cluster_names:
        r[ROUTE_DEFAULT_INTENT].add(cluster_name)
    return r


def _get_force_enable_dest_apps(from_application_name, request_data):
    for type_name, application_names in request_data.iteritems():
        if type_name != SERVICE_SUBDOMAIN:
            continue
        for name in application_names:
            if _force_enable_route_for_dest(from_application_name, name):
                yield name


def _force_enable_route_for_dest(from_app, dest_app):
    if dest_app not in settings.ROUTE_FORCE_ENABLE_DEST_APPS:
        return False

    exclude_source_map = settings.ROUTE_FORCE_ENABLE_EXCLUDE_SOURCE_MAP
    exclude_source_pairs = sorted(
        exclude_source_map.iteritems(),
        key=lambda x: int('*' in x[0]))
    exclude_source_apps = []

    for dest_app_match, exclude_list in exclude_source_pairs:
        if dest_app == dest_app_match:
            exclude_source_apps = exclude_list
            break
        if dest_app_match.endswith('*') and dest_app.startswith(
                dest_app_match[:-1]):
            exclude_source_apps = exclude_list
            break

    return from_app not in exclude_source_apps


def _get_ezone(request_domain, application_name, cluster_name, request_addr):
    ezone = settings.ROUTE_DOMAIN_EZONE_MAP.get(request_domain, '')
    if ezone not in settings.ROUTE_EZONE_DEFAULT_HIJACK_MODE:
        logger.info(
            'unknown domain: %s %s %s %r', request_addr, request_domain,
            application_name, cluster_name)
        monitor_client.increment('route_hijack.unknown_domain', tags={
            'domain': request_domain,
            'from_application_name': application_name,
            'appid': application_name,
        })
        if not cluster_name:
            logger.info('unknown domain and cluster: %s %s %s',
                        request_addr, request_domain, application_name)
            monitor_client.increment('route_hijack.unknown_cluster', tags={
                'domain': request_domain,
                'from_application_name': application_name,
                'appid': application_name,
            })
            return settings.EZONE or 'default'
        if not switch.is_switched_on(
                SWITCH_ENABLE_ROUTE_HIJACK_WITH_LOCAL_EZONE, False):
            return settings.EZONE or 'default'

        ezone = try_to_extract_ezone(cluster_name, default='')
        if not ezone:
            return settings.ROUTE_OVERALL_EZONE

    return ezone


def _get_route_hijack_list(from_application_name, ezone):
    route_stage_table = lookup_route_stage()
    cluster_name = settings.ROUTE_EZONE_CLUSTER_MAP.get(ezone)
    if cluster_name is None:
        return settings.ROUTE_HIJACK_LIST

    hijack_mode = route_stage_table.get(
        from_application_name, {}).get(cluster_name)
    if not hijack_mode:
        return {}

    return {from_application_name: hijack_mode}
