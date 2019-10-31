from __future__ import absolute_import

from pytest import fixture, raises

from huskar_api.models.auth import (
    User, Team, TeamAdmin, Application, Authority)
from huskar_api.models.auth.team import TeamNotEmptyError
from huskar_api.models.exceptions import NameOccupiedError


@fixture
def team_foo(db):
    return Team.create('foo')


@fixture
def team_bar(db):
    return Team.create('bar')


@fixture
def user_foo(db):
    user = User(username='elder.frog', password='x', is_app=False)
    db.add(user)
    db.commit()
    return user


@fixture
def user_bar(db):
    user = User(username='bar.frog', password='x', is_app=False)
    db.add(user)
    db.commit()
    return user


def test_create_team(db):
    team = Team.create('bar')
    assert team.id > 0
    assert team.team_name == 'bar'

    with raises(NameOccupiedError):
        Team.create('bar')  # name conflicts

    team = Team.create('baz')
    assert team.team_name == 'baz'


def test_delete_team(team_foo, team_bar, user_foo):
    Application.create('biu', team_bar.id)
    TeamAdmin.ensure(team_foo.id, user_foo.id)

    # fill cache
    assert Team.get_by_name(team_foo.team_name) is team_foo
    assert Team.get_by_name(team_bar.team_name) is team_bar
    assert TeamAdmin.get_user_ids(team_foo.id) == [user_foo.id]

    Team.delete(team_foo.id)
    with raises(TeamNotEmptyError):
        Team.delete(team_bar.id)

    assert Team.get_by_name(team_foo.team_name) is None
    assert Team.get_by_name(team_bar.team_name) is not None
    assert Team.get(team_foo.id) is None
    assert Team.get(team_bar.id) is not None
    assert TeamAdmin.get_user_ids(team_foo.id) == []
    Team.delete(team_foo.id)
    assert Team.get(team_foo.id) is None


def test_get_team_by_name(db, team_foo):
    team = Team.get_by_name(team_foo.team_name)
    assert team is team_foo

    team = Team.get_by_name(team_foo.team_name + '1s')
    assert team is None


def test_get_teams_by_admin(db, team_foo, team_bar, user_foo, user_bar):
    assert Team.get_multi_by_admin(user_foo.id) == []
    assert Team.get_multi_by_admin(user_bar.id) == []

    team_foo.grant_admin(user_foo.id)
    team_bar.grant_admin(user_foo.id)
    team_bar.grant_admin(user_bar.id)

    assert Team.get_multi_by_admin(user_foo.id) == [team_foo, team_bar]
    assert Team.get_multi_by_admin(user_bar.id) == [team_bar]


def test_get_all_teams(db, team_foo, team_bar):
    assert Team.get_all() == [team_foo, team_bar]

    team_baz = Team.create('baz')

    assert Team.get_all() == [team_foo, team_bar, team_baz]


def test_grant_admin(db, team_foo, user_foo, user_bar):
    assert team_foo.list_admin() == []
    team_foo.grant_admin(user_foo.id)
    assert team_foo.list_admin() == [user_foo]

    team_foo.grant_admin(user_foo.id)

    assert team_foo.list_admin() == [user_foo]
    team_foo.grant_admin(user_bar.id)
    assert set(team_foo.list_admin()) == {user_foo, user_bar}


def test_dismiss_admin(
        db, team_foo, user_foo, user_bar):
    team_foo.grant_admin(user_foo.id)
    team_foo.grant_admin(user_bar.id)

    assert set(team_foo.list_admin()) == {user_foo, user_bar}

    team_foo.dismiss_admin(user_bar.id)
    assert team_foo.list_admin() == [user_foo]

    team_foo.dismiss_admin(user_bar.id)
    assert team_foo.list_admin() == [user_foo]

    team_foo.dismiss_admin(user_foo.id)
    assert team_foo.list_admin() == []


def test_check_admin(db, team_foo, user_foo, user_bar):
    team_foo.grant_admin(user_foo.id)
    team_foo.grant_admin(user_bar.id)

    assert team_foo.check_auth(Authority.WRITE, user_bar.id)

    team_foo.dismiss_admin(user_bar.id)
    assert team_foo.check_auth(Authority.WRITE, user_foo.id)
    assert not team_foo.check_auth(Authority.WRITE, user_bar.id)

    team_foo.dismiss_admin(user_bar.id)
    assert team_foo.check_auth(Authority.WRITE, user_foo.id)
    assert not team_foo.check_auth(Authority.WRITE, user_bar.id)

    team_foo.dismiss_admin(user_foo.id)
    assert not team_foo.check_auth(Authority.WRITE, user_foo.id)
    assert not team_foo.check_auth(Authority.WRITE, user_bar.id)


def test_archive_team(db, team_foo, user_foo, user_bar):
    team_foo.grant_admin(user_foo.id)
    assert team_foo.is_active is True

    application = Application.create('biu', team_foo.id)
    assert application.is_active is True
    with raises(TeamNotEmptyError):
        team_foo.archive()

    application.archive()
    team_foo.archive()
    assert Team.get_by_name(team_foo.team_name) is None
    assert db.query(TeamAdmin.id).filter_by(
        team_id=team_foo.id, user_id=user_foo.id).first() is not None


def test_unarchive(db, team_foo, user_foo, user_bar):
    team_foo.archive()
    assert Team.get_by_name(team_foo.team_name) is None
    team_foo.unarchive()
    assert Team.get_by_name(team_foo.team_name) is not None


def test_rename_desc(db, team_foo):
    new_desc = u'test-test'
    team_foo.rename_desc(new_desc)
    assert team_foo.team_desc == new_desc
