from __future__ import absolute_import

from huskar_api.models.alembic import get_metadata


def test_metadata():
    assert get_metadata()
