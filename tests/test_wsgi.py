from __future__ import absolute_import

from pytest import raises
from decouple import UndefinedValueError

import huskar_api.wsgi
import huskar_api.settings


def test_wsgi_entry():
    assert callable(huskar_api.wsgi.app)


def test_settings():
    with raises(UndefinedValueError):
        huskar_api.settings.config.get('NOT_EXISTS')
    assert huskar_api.settings.config.get('NOT_EXISTS', default=None) is None
    assert huskar_api.settings.config_repository.get('NOT_EXISTS') is None
