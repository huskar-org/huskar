from __future__ import absolute_import

import gevent.monkey; gevent.monkey.patch_all()  # noqa

import contextlib
import datetime
import gevent.monkey

import requests_mock
from pytest import fixture
from mock import create_autospec
from sqlalchemy.engine import create_engine
from werkzeug.utils import import_string

from huskar_api.app import create_app
from huskar_api.models import (
    DeclarativeBase, DBSession, cache_manager, huskar_client)
from huskar_api.switch import switch


gevent.monkey.patch_all()


@fixture
def app(mocker):
    return create_app()


@fixture
def client(app):
    return app.test_client()


@fixture(autouse=True)
def secret_key(mocker):
    return mocker.patch('huskar_api.settings.SECRET_KEY', 'foobar')


@fixture(autouse=True)
def debug(mocker):
    return mocker.patch('huskar_api.settings.DEBUG', True)


@fixture(autouse=True)
def testing(mocker):
    return mocker.patch('huskar_api.settings.TESTING', True)


@fixture(autouse=True)
def fallback_secret_keys(mocker):
    return mocker.patch(
        'huskar_api.settings.FALLBACK_SECRET_KEYS', ['foo', 'bar'])


@fixture(autouse=True)
def admin_home_url(mocker):
    return mocker.patch(
        'huskar_api.settings.ADMIN_HOME_URL', 'http://example.com')


@fixture(autouse=True)
def admin_signup_url(mocker):
    return mocker.patch(
        'huskar_api.settings.ADMIN_SIGNUP_URL', 'http://example.com/ewf')


@fixture(autouse=True)
def admin_reset_password_url(mocker):
    return mocker.patch(
        'huskar_api.settings.ADMIN_RESET_PASSWORD_URL',
        'http://example.com/password-reset/{username}/{token}')


@fixture(autouse=True)
def admin_infra_config_url(mocker):
    return mocker.patch(
        'huskar_api.settings.ADMIN_INFRA_CONFIG_URL',
        'http://example.com/application/{application_name}/config?'
        'infra_type={infra_type}&infra_name={infra_name}')


@fixture(autouse=True)
def admin_emergency_user_list(mocker):
    return mocker.patch(
        'huskar_api.settings.ADMIN_EMERGENCY_USER_LIST', ['admin'])


@fixture(autouse=True)
def auth_blacklist(mocker):
    return mocker.patch(
        'huskar_api.settings.AUTH_IP_BLACKLIST', frozenset(['169.254.0.255']))


@fixture(autouse=True)
def route_ezone_list(mocker):
    return mocker.patch(
        'huskar_api.settings.ROUTE_EZONE_LIST', ['alta1', 'altb1'])


@fixture(autouse=True)
def monitor_client(mocker):
    obj_path = 'huskar_api.extras.monitor.monitor_client'
    monitor_client = import_string(obj_path)
    spec = create_autospec(monitor_client, spec_set=True)
    for name, mock in spec._mock_children.items():
        mocker.patch('%s.%s' % (obj_path, name), mock)
    return monitor_client


@fixture
def zk():
    if not huskar_client.start():
        raise RuntimeError('ZooKeeper has gone away')
    return huskar_client.client


@fixture
def redis_client():
    client = cache_manager.make_client(raw=True)
    try:
        _redis_flushall(client)
        yield client
    finally:
        _redis_flushall(client)


@fixture(autouse=True, scope='function')
def db(redis_client):
    def truncate_tables(session):
        for table in reversed(DeclarativeBase.metadata.sorted_tables):
            if table.name.startswith('alembic'):
                continue
            session.execute(table.delete())
        session.commit()

    try:
        with contextlib.closing(DBSession()) as session:
            truncate_tables(session)
            yield session
    finally:
        with contextlib.closing(DBSession()) as session:
            truncate_tables(session)


@fixture
def ceiled_now():
    # We use this to compare with MySQL datetime.
    now = datetime.datetime.now()
    return now.replace(microsecond=0) + datetime.timedelta(seconds=1)  # +1s


@fixture
def broken_db(db):
    broken_engine = create_engine(
        'mysql+pymysql://root@127.0.0.1:1/dotdotdot?charset=utf8')
    broken_engines = {'master': broken_engine, 'slave': broken_engine}
    healthy_engines = dict(db.engines)
    try:
        DBSession.registry.registry.clear()
        DBSession.configure(engines=broken_engines)
        yield DBSession()
    finally:
        DBSession.registry.registry.clear()
        DBSession.configure(engines=healthy_engines)


@fixture(params=[False, True])
def minimal_mode(request, mocker, redis_client):
    if request.param is True:
        request.getfixturevalue('broken_db')
        _redis_flushall(redis_client)
    return request.param


@fixture
def req_mocker():
    with requests_mock.mock() as mocker:
        yield mocker


@fixture
def redis_flushall():
    return _redis_flushall


def _redis_flushall(client):
    keys = client.keys('huskar_api.*')
    pipeline = client.pipeline()
    for key in keys:
        pipeline.delete(key)
    pipeline.execute()


@fixture
def mock_switches(mocker):
    def wrapper(switches):
        raw_is_switched_on = switch.is_switched_on
        switches = switches or {}

        def _fake_switch(key, default=False):
            value = switches.get(key)
            if value is not None:
                return value
            return raw_is_switched_on(key, default)
        mocker.patch.object(switch, 'is_switched_on', _fake_switch)
    return wrapper
