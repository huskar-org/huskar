from __future__ import absolute_import

import base64

import simplejson as json
from freezegun import freeze_time
from pytest import fixture, mark, raises
from itsdangerous import BadSignature, SignatureExpired

from huskar_api.models.auth import User
from huskar_api.models.exceptions import NameOccupiedError


@fixture
def user_foo(db):
    user = User(username='foo', password='*')
    db.add(user)
    db.commit()
    User.get(user.id, force=True)  # touch cache
    return user


@fixture
def user_bar(db):
    user = User(username='bar', password='*', is_app=True)
    db.add(user)
    db.commit()
    User.get(user.id, force=True)  # touch cache
    return user


def test_create_normal(db):
    user = User.create_normal('water', 'foobar', 'water@foo.bar')
    assert user.username == 'water'
    assert user.check_password('foobar')
    assert not user.check_password('foobaz')
    assert not user.is_active
    assert user.is_admin is False
    assert user.is_application is False

    with raises(NameOccupiedError):
        User.create_normal('water', 'foobaz', 'water2@foo.bar')
    assert db.query(User.email).filter_by(
        username='water').scalar() == 'water@foo.bar'


def test_create_application(db):
    user = User.create_application('base.foo')
    assert user.username == 'base.foo'
    assert user.email is None
    assert user.is_active
    assert not user.huskar_admin
    assert user.is_admin is False
    assert user.is_application is True

    with db.close_on_exit(False):
        user.email = 'dummy@foo.bar'
        user.is_active = False
    User.flush([user.id])

    old_attributes = dict(vars(user))

    user_shadow = User.create_application('base.foo')
    assert user_shadow is user
    assert user_shadow.id == old_attributes['id']
    assert user_shadow.email is None
    assert user_shadow.is_active
    assert not user_shadow.huskar_admin
    assert user_shadow.email != old_attributes['email']
    assert user_shadow.username == old_attributes['username']
    assert user_shadow.password == old_attributes['password']
    assert user_shadow.created_at == old_attributes['created_at']

    with db.close_on_exit(False):
        user.huskar_admin = True
    User.flush([user.id])

    user_shadow_foo = User.create_application('base.foo')
    assert user_shadow_foo is user
    assert user_shadow_foo.id == old_attributes['id']
    assert user_shadow_foo.email is None
    assert user_shadow_foo.is_active
    assert user_shadow_foo.huskar_admin
    assert user_shadow_foo.username == old_attributes['username']
    assert user_shadow_foo.password == old_attributes['password']
    assert user_shadow_foo.created_at == old_attributes['created_at']


def test_get_user_by_name(db, user_foo):
    user = User.get_by_name(user_foo.username)
    assert user is user_foo

    user = User.get_by_name(user_foo.username + '1s')
    assert user is None


def test_get_all_normal(db, user_foo):
    assert User.get_all_normal() == [user_foo]

    user_bar = User.create_application('base.bar')
    user_baz = User.create_normal('baz', '-', 'baz@foo.bar', is_active=True)
    user_too = User.create_normal('too', '-', 'too@foo.bar', is_active=False)

    assert User.get_all_normal() == [user_foo, user_baz]
    assert user_bar not in User.get_all_normal()
    assert user_too not in User.get_all_normal()


def test_archive(db, user_foo):
    assert User.get_all_normal() == [user_foo]
    assert User.get_by_name(user_foo.username) is not None
    user_foo.archive()
    assert User.get_all_normal() == []
    assert User.get_by_name(user_foo.username) is None


@mark.xparametrize
def test_change_password(db, user_foo, input_password, hashed_password):
    user_foo.change_password(input_password)
    assert db.query(User.password).filter_by(
        username=user_foo.username).scalar() == hashed_password

    db.close()  # clear identity map
    assert User.get(user_foo.id).password == hashed_password


@mark.xparametrize
def test_check_password(db, user_foo, present_password, input_password):
    user_foo.password = present_password
    db.commit()

    assert user_foo.check_password(input_password['correct'])
    assert not user_foo.check_password(input_password['incorrect'])


def test_grant_admin(db, user_foo):
    user_foo.grant_admin()
    assert user_foo.is_admin


@mark.parametrize('present_is_admin', [True, False])
def test_dismiss_admin(db, user_foo, present_is_admin):
    user_foo.huskar_admin = present_is_admin
    db.commit()

    user_foo.dismiss_admin()
    assert user_foo.is_admin is False
    assert User.get(user_foo.id, force=True).is_admin is False


def test_application_methods(user_bar):
    with raises(NotImplementedError):
        user_bar.change_password('any')

    with raises(NotImplementedError):
        user_bar.check_password('any')

    with raises(NotImplementedError):
        user_bar.grant_admin()

    with raises(NotImplementedError):
        user_bar.dismiss_admin()


@mark.parametrize('is_app', [True, False])
def test_generate_token(user_foo, is_app):
    user_foo.is_app = is_app

    token = user_foo.generate_token('42')
    parsed_token = token.split('.')
    assert len(parsed_token) == 3

    header = json.loads(base64.urlsafe_b64decode(parsed_token[0] + '=='))
    payload = json.loads(base64.urlsafe_b64decode(parsed_token[1]))
    sign = base64.urlsafe_b64decode(parsed_token[2] + '==')

    assert header['alg'] == 'HS256'
    assert header['exp'] == float('inf')
    assert payload['username'] == user_foo.username
    assert len(sign) == 32


@mark.parametrize('is_app', [True, False])
def test_parse_token(user_foo, is_app):
    user_foo.is_app = is_app

    token = user_foo.generate_token('42')
    assert user_foo.get_by_token('42', token) is user_foo
    assert user_foo.get_by_token('43', token) is None
    assert user_foo.get_by_token('42', token + '1s') is None
    with freeze_time('9999-12-31 23:59:59'):
        assert user_foo.get_by_token('42', token) is user_foo

    with raises(BadSignature):
        user_foo.get_by_token('43', token, raises=True)

    with raises(BadSignature):
        user_foo.get_by_token('42', token + '1s', raises=True)


@mark.parametrize('is_app', [True, False])
def test_parse_expired_token(user_foo, is_app):
    token = user_foo.generate_token('42', expires_in=100)
    assert user_foo.get_by_token('42', token) is user_foo

    with freeze_time('9999-12-31 23:59:59'):
        assert user_foo.get_by_token('42', token) is None

    with raises(SignatureExpired), freeze_time('9999-12-31 23:59:59'):
        user_foo.get_by_token('42', token, raises=True)


def test_unarchive(db, user_foo):
    user_foo.archive()
    assert User.get_by_name(user_foo.username) is None
    user_foo.unarchive()
    assert User.get_by_name(user_foo.username) is not None
