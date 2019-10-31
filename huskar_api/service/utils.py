from __future__ import absolute_import

from huskar_api.settings import ROUTE_INTENT_LIST
from huskar_api.models.utils import normalize_cluster_name
from .exc import DuplicatedEZonePrefixError, ClusterNameUnsupportedError


def check_cluster_name(cluster_name, application_name="unknown"):
    if normalize_cluster_name(cluster_name) != cluster_name:
        raise DuplicatedEZonePrefixError(
            'Cluster name should not contain duplicated E-Zone prefix.')

    return cluster_name


def check_cluster_name_in_creation(cluster_name, application_name="unknown"):
    if cluster_name in ROUTE_INTENT_LIST:
        raise ClusterNameUnsupportedError(
            'Cluster name "{}" are not allowed in Huskar.'.format(
                cluster_name
            )
        )

    if normalize_cluster_name(cluster_name) != cluster_name:
        raise DuplicatedEZonePrefixError(
            'Cluster name should not contain duplicated E-Zone prefix.')

    return cluster_name
