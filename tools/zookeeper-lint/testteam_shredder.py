from __future__ import absolute_import

import logging

from huskar_sdk_v2.consts import (
    SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN)

from huskar_api.models import huskar_client
from huskar_api.models.auth import Application
from huskar_api.models.manifest import application_manifest


NAME_PREFIXES = ('testteam',)


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.propagate = False


def collect_applications():
    names = set()
    names.update(x.application_name for x in Application.get_all())
    names.update(application_manifest.as_list())
    return sorted(n for n in names if n.startswith(NAME_PREFIXES))


def destroy_application(name):
    application = Application.get_by_name(name)
    if application is not None:
        Application.delete(application.id)
        logger.info('Removed application from DBMS: %s', name)

    for type_name in (SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN):
        path = '/huskar/%s/%s' % (type_name, name)
        if huskar_client.client.exists(path):
            huskar_client.client.delete(path, recursive=True)
            logger.info('Removed application from ZooKeeper: %s', path)


def main():
    application_names = collect_applications()
    logger.info('Collected %d applications', len(application_names))
    for application_name in application_names:
        logger.info('Processing %s', application_name)
        destroy_application(application_name)


if __name__ == '__main__':
    main()
