from __future__ import absolute_import

import logging
import collections

from huskar_api.models import huskar_client
from huskar_api.models.auth import Application
from huskar_api.models.infra import InfraDownstream, extract_application_names
from huskar_api.models.instance import InfraInfo
from huskar_api.models.const import INFRA_CONFIG_KEYS


logger = logging.getLogger(__name__)


InfraUpstreamInfo = collections.namedtuple('InfraUpstreamInfo', [
    'infra_type',
    'infra_name',
    'scope_type',
    'scope_name',
    'field_name',
    'infra_application_name',
])


def _collect_infra_upstream(client, application_name):
    for infra_type in INFRA_CONFIG_KEYS:
        try:
            infra_info = InfraInfo(client, application_name, infra_type)
        except ValueError:
            logger.exception(
                'ValueError with appid: %s type: %s',
                application_name, infra_type)
            continue
        infra_info.load()
        infra_data = infra_info.data or {}
        for scope_type, scope_data in infra_data.iteritems():
            for scope_name, scope_dict in scope_data.iteritems():
                for infra_name, value in scope_dict.iteritems():
                    infra_urls = infra_info.extract_urls(value, as_dict=True)
                    infra_applications = extract_application_names(infra_urls)
                    for field_name, infra_application_name in \
                            infra_applications.iteritems():
                        yield InfraUpstreamInfo(
                            infra_type,
                            infra_name,
                            scope_type,
                            scope_name,
                            field_name,
                            infra_application_name)


def _bind_infra_upstream(builder, iterator, application_name):
    for upstream in sorted(frozenset(iterator)):
        builder.bind(
            application_name,
            upstream.infra_type,
            upstream.infra_name,
            upstream.scope_type,
            upstream.scope_name,
            upstream.field_name,
            upstream.infra_application_name)
        yield upstream


def collect_infra_config():
    logger.info('Looking up application list')
    application_list = Application.get_all()
    builder = InfraDownstream.bindmany()
    client = huskar_client.client

    for application in application_list:
        application_name = application.application_name
        logger.info('Collecting %s', application_name)
        iterator = _collect_infra_upstream(client, application_name)
        iterator = _bind_infra_upstream(builder, iterator, application_name)
        for upstream in iterator:
            logger.info('Recorded %r', upstream)
            InfraDownstream.flush_cache_by_application(
                upstream.infra_application_name)
        builder.commit()
        logger.info('Committed %s', application_name)

    logger.info('Deleting stale records')
    builder.unbind_stale().commit()
    logger.info('Done')
