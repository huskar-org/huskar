from __future__ import absolute_import

from huskar_api.ext import EnhancedSentry


def test_sentry_init_app_client_none(mocker):
    mocked_client = mocker.MagicMock(return_value=None)
    mocked_client.__bool__ = mocker.MagicMock(return_value=False)
    mocker.patch('raven.contrib.flask.make_client', mocked_client)
    sentry = EnhancedSentry()
    sentry.register_signal = False
    sentry.client = None
    sentry.init_app(mocker.MagicMock(__name__='app'))
    assert not sentry.client
