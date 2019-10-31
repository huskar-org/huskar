from __future__ import absolute_import

import raven

from pytest import fixture

from huskar_api.switch import (
    switch, SWITCH_ENABLE_SENTRY_MESSAGE, SWITCH_ENABLE_SENTRY_EXCEPTION)
from huskar_api.extras import raven as huskar_raven
from huskar_api.extras.raven import capture_message, capture_exception


@fixture
def turn_off(mocker):
    def turn_off(name):
        def is_switched_on(switch_name, default=True):
            if switch_name == name:
                return False
            return default
        return mocker.patch.object(switch, 'is_switched_on', is_switched_on)
    return turn_off


@fixture
def sentry_client(mocker):
    c = mocker.patch.object(
        huskar_raven, 'raven_client',
        raven.Client(dns='gevent+http://foo:bar@example.com/1'))
    return c


def test_message_on(sentry_client, mocker):
    func = mocker.patch.object(
        sentry_client, 'captureMessage', autospec=True)
    capture_message('foobar')
    func.assert_called_once_with('foobar')


def test_message_off(sentry_client, mocker, turn_off):
    func = mocker.patch.object(
        sentry_client, 'captureMessage', autospec=True)
    turn_off(SWITCH_ENABLE_SENTRY_MESSAGE)
    capture_message('foobar')
    assert not func.called


def test_exception_on(sentry_client, mocker):
    func = mocker.patch.object(
        sentry_client, 'captureException', autospec=True)
    mocker.patch('huskar_api.extras.raven.raven_client', sentry_client)
    capture_exception()
    func.assert_called_once_with()


def test_exception_off(sentry_client, mocker, turn_off):
    func = mocker.patch.object(
        sentry_client, 'captureException', autospec=True)
    turn_off(SWITCH_ENABLE_SENTRY_EXCEPTION)
    capture_exception()
    assert not func.called


def test_ignore_send_error(mocker):
    def is_switched_on(switch_name, default=True):
        return default

    mocker.patch.object(switch, 'is_switched_on', is_switched_on)

    mocker.patch('huskar_api.extras.raven.raven_client',
                 mocker.MagicMock(
                     captureMessage=mocker.MagicMock(
                         side_effect=Exception),
                     captureException=mocker.MagicMock(
                         side_effect=Exception)))

    assert capture_message('test') is None
    assert capture_exception('error') is None
