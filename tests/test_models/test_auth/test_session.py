from __future__ import absolute_import

from pytest import fixture

from huskar_api import settings
from huskar_api.models.auth import User
from huskar_api.models.auth.session import SessionAuth


@fixture
def session_auth():
    return SessionAuth('san.zhang')


@fixture
def create_user(db):
    def create(username):
        user = User(username=username, password='*')
        db.add(user)
        db.commit()
        User.get(user.id, force=True)  # touch cache
        return user

    return create


def test_minimal_mode_metrics(session_auth, monitor_client):
    assert session_auth.is_minimal_mode is False
    assert session_auth.minimal_mode_reason is None
    monitor_client.increment.assert_not_called()

    session_auth.enter_minimal_mode('tester')
    session_auth.enter_minimal_mode()  # ignored
    session_auth.enter_minimal_mode()  # ignored
    assert session_auth.is_minimal_mode is True
    assert session_auth.minimal_mode_reason == 'tester'
    monitor_client.increment.assert_called_once_with('minimal_mode.qps', 1)


def test_switch_as(create_user):
    user_foo = create_user('foo')
    auth = SessionAuth(user_foo.username)
    assert repr(auth) == 'SessionAuth(%r)' % 'foo'
    auth.load_user()
    assert auth.id == user_foo.id

    user_bar = create_user('bar')
    with auth.switch_as(user_bar.username):
        assert auth.id == user_bar.id

    assert auth.id == user_foo.id


def test_update_admin_emergency_user_list():
    old_data = settings.ADMIN_EMERGENCY_USER_LIST
    new_data = ['a', 'foo', 'foobar']
    try:
        settings.update_admin_emergency_user_list(new_data)
        assert settings.ADMIN_EMERGENCY_USER_LIST == frozenset(new_data)
    finally:
        settings.ADMIN_EMERGENCY_USER_LIST = old_data
