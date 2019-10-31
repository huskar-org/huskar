from __future__ import absolute_import

from pytest import fixture

from huskar_api.models import huskar_client
from huskar_api.models.tree import TreeHub


@fixture
def test_application_name(faker):
    return faker.uuid4()


@fixture
def hub():
    return TreeHub(huskar_client)


@fixture
def holder(hub, test_application_name):
    holder = hub.get_tree_holder(test_application_name, 'config')
    holder.block_until_initialized(5)
    return holder


@fixture
def service_holder(hub, test_application_name):
    holder = hub.get_tree_holder(test_application_name, 'service')
    holder.block_until_initialized(5)
    return holder
