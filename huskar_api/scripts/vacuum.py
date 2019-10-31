from __future__ import absolute_import

import logging

from huskar_sdk_v2.consts import (
    SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN)

from huskar_api import settings
from huskar_api.models import huskar_client
from huskar_api.models.manifest import application_manifest
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.container import ContainerManagement
from huskar_api.models.exceptions import (
    NotEmptyError, OutOfSyncError, MalformedDataError)


logger = logging.getLogger(__name__)


def _vacuum_empty_clusters(type_name):
    for application_name in application_manifest.as_list():
        if application_name in settings.AUTH_APPLICATION_BLACKLIST:
            continue
        logger.info('[%s] Check application %s', type_name, application_name)
        im = InstanceManagement(huskar_client, application_name, type_name)
        try:
            for cluster_name in im.list_cluster_names():
                ident = (application_name, type_name, cluster_name)
                try:
                    im.delete_cluster(cluster_name)
                except OutOfSyncError:
                    logger.info('Skip %r because of changed version.', ident)
                except NotEmptyError as e:
                    logger.info(
                        'Skip %r because %s.', ident, e.args[0].lower())
                except MalformedDataError:
                    logger.info('Skip %r because of unrecognized data.', ident)
                else:
                    logger.info('Okay %r is gone.', ident)
        except Exception as e:
            logger.exception('Skip %s because %s.', application_name, e)


def vacuum_empty_clusters():
    logger.info('Begin to vacuum empty clusters')
    for type_name in (
            SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN):
        _vacuum_empty_clusters(type_name)
    logger.info('Done to vacuum empty clusters')


def vacuum_stale_barriers():
    logger.info('Begin to vacuum stale container barriers')
    vacuum_iterator = ContainerManagement.vacuum_stale_barriers(huskar_client)
    for container_id, is_stale in vacuum_iterator:
        if is_stale:
            logger.info('Delete stale barrier of container %s', container_id)
        else:
            logger.info('Skip barrier of container %s', container_id)
    logger.info('Done to vacuum stale container barriers')
