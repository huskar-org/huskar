from __future__ import absolute_import

from pytest import fixture, raises, mark

from huskar_api.models.auth import (
    Team, User, Application, ApplicationAuth, Authority)
from huskar_api.models.exceptions import NameOccupiedError, OutOfSyncError
from huskar_api.models.catalog.schema import ServiceInfo


@fixture
def test_user(db):
    user = User(username='elder.frog', password='x', is_app=False)
    db.add(user)
    db.commit()
    return user


@fixture
def test_team(db):
    team = Team(team_name='Foo', team_desc='foo-bar')
    db.add(team)
    db.commit()
    return team


@fixture
def test_application(db, test_team):
    app = Application(application_name='foo.frog', team_id=test_team.id)
    db.add(app)
    db.commit()
    return app


@fixture
def test_application_auth(db, test_user, test_application):
    auth = ApplicationAuth(
        authority=Authority.READ.value, user_id=test_user.id,
        application_id=test_application.id)
    db.add(auth)
    db.commit()
    return auth


@fixture
def query_application_auth(db):
    def func():
        return db.query(
            ApplicationAuth.authority,
            ApplicationAuth.user_id,
            ApplicationAuth.application_id,
        ).order_by(ApplicationAuth.id.desc()).all()
    return func


@mark.parametrize('ttl,result_expected', [
    (0, -1L),
    (100, 100),
])
def test_table_cache_expiration(
        faker, monkeypatch, db, test_team, ttl, result_expected):
        monkeypatch.setattr(Application, 'TABLE_CACHE_EXPIRATION_TIME', ttl)
        application_name = faker.uuid4()[:8]
        application = Application.create(application_name, test_team.id)
        assert application

        cache_client = Application._cache_client
        raw_key = Application.gen_raw_key(application.id)
        key = cache_client._keygen(raw_key)
        assert int(cache_client.client.ttl(key)) == result_expected


def test_ensure_auth(db, test_application_auth, test_user, test_application,
                     query_application_auth):
    assert query_application_auth() == [
        (Authority.READ.value, test_user.id, test_application.id),
    ]

    test_application.ensure_auth(Authority.READ, test_user.id)
    assert query_application_auth() == [
        (Authority.READ.value, test_user.id, test_application.id),
    ]

    test_application.ensure_auth(Authority.WRITE, test_user.id)
    assert query_application_auth() == [
        (Authority.WRITE.value, test_user.id, test_application.id),
        (Authority.READ.value, test_user.id, test_application.id),
    ]


def test_ensure_auth_with_invalid_argument(db, test_application, test_user,
                                           query_application_auth):
    assert query_application_auth() == []
    with raises(AssertionError):
        test_application.ensure_auth('unknow', test_user.id)
    assert query_application_auth() == []


def test_discard_auth(db, test_application_auth, test_user, test_application,
                      query_application_auth):
    assert query_application_auth() == [
        (Authority.READ.value, test_user.id, test_application.id),
    ]

    test_application.discard_auth(Authority.WRITE, test_user.id)
    assert query_application_auth() == [
        (Authority.READ.value, test_user.id, test_application.id),
    ]

    test_application.discard_auth(Authority.READ, test_user.id)
    assert query_application_auth() == []


def test_discard_auth_with_invalid_argument(db, test_application, test_user,
                                            query_application_auth):
    assert query_application_auth() == []
    with raises(AssertionError):
        test_application.discard_auth('unknow', test_user.id)
    assert query_application_auth() == []


def test_list_auth(db, test_application_auth, test_application, test_user):
    auth_user = test_application.setup_default_auth()
    auth_list = test_application.list_auth()
    assert len(auth_list) == 2
    assert auth_list[0].user_id == auth_user.id
    assert auth_list[0].authority == Authority.WRITE.value
    assert auth_list[1].user_id == test_user.id
    assert auth_list[1].authority == Authority.READ.value


def test_search_auth(db, test_user, test_application):
    auth_r = ApplicationAuth(
        authority=Authority.READ.value, user_id=test_user.id,
        application_id=test_application.id)
    auth_w = ApplicationAuth(
        authority=Authority.WRITE.value, user_id=test_user.id,
        application_id=test_application.id)
    db.add_all([auth_r, auth_w])
    db.commit()

    assert ApplicationAuth.search_by(authority=Authority.READ) == [auth_r]
    assert ApplicationAuth.search_by(
        authority=Authority.READ, user_id=test_user.id) == [auth_r]
    assert ApplicationAuth.search_by(
        authority=Authority.READ, user_id=test_user.id,
        application_id=test_application.id) == [auth_r]
    assert ApplicationAuth.search_by(authority=Authority.WRITE) == [auth_w]
    assert ApplicationAuth.search_by(
        user_id=test_user.id, application_id=test_application.id,
    ) == [auth_w, auth_r]


def test_find_auth(db, test_application_auth, test_user, test_application):
    assert ApplicationAuth.find(
        Authority.READ, test_user.id, test_application.id
    ) is test_application_auth
    assert test_application_auth.user is test_user
    assert not ApplicationAuth.find(
        Authority.WRITE, test_user.id, test_application.id)
    assert not ApplicationAuth.find(
        Authority.ADMIN, test_user.id, test_application.id)


def test_check_auth_for_granted_user(db, test_user, test_application):
    assert not test_application.check_auth(Authority.READ, test_user.id)
    assert not test_application.check_auth(Authority.WRITE, test_user.id)
    assert not test_application.check_auth(Authority.ADMIN, test_user.id)

    test_application.ensure_auth(Authority.WRITE, test_user.id)

    assert test_application.check_auth(Authority.READ, test_user.id)
    assert test_application.check_auth(Authority.WRITE, test_user.id)
    assert not test_application.check_auth(Authority.ADMIN, test_user.id)


def test_check_auth_for_team_admin(
        db, test_user, test_application):
    assert not test_application.check_auth(Authority.READ, test_user.id)
    assert not test_application.check_auth(Authority.WRITE, test_user.id)
    assert not test_application.check_auth(Authority.ADMIN, test_user.id)

    test_application.team.grant_admin(test_user.id)

    assert test_application.check_auth(Authority.READ, test_user.id)
    assert test_application.check_auth(Authority.WRITE, test_user.id)
    assert test_application.check_auth(Authority.ADMIN, test_user.id)

    test_application.team.dismiss_admin(test_user.id)

    assert not test_application.check_auth(Authority.READ, test_user.id)
    assert not test_application.check_auth(Authority.WRITE, test_user.id)


def test_check_auth_for_site_admin(
        db, test_user, test_application):
    assert not test_application.check_auth(Authority.READ, test_user.id)
    assert not test_application.check_auth(Authority.WRITE, test_user.id)
    assert not test_application.check_auth(Authority.ADMIN, test_user.id)

    test_user.grant_admin()

    assert test_application.check_auth(Authority.READ, test_user.id)
    assert test_application.check_auth(Authority.WRITE, test_user.id)
    assert test_application.check_auth(Authority.ADMIN, test_user.id)

    test_user.dismiss_admin()

    assert not test_application.check_auth(Authority.READ, test_user.id)
    assert not test_application.check_auth(Authority.WRITE, test_user.id)
    assert not test_application.check_auth(Authority.ADMIN, test_user.id)


def test_check_auth_with_invalid_argument(db, test_user, test_application):
    with raises(AssertionError):
        test_application.check_auth('voldemort', test_user.id)


def test_get_application_by_name(db, test_application):
    application = Application.get_by_name(test_application.application_name)
    assert application is test_application
    assert application.domain_name == 'foo'

    application = Application.get_by_name(
        test_application.application_name + '1s')
    assert application is None


def test_get_application_list(db, test_application):
    application_list = Application.get_all()
    assert application_list == [test_application]


def test_get_application_list_by_team(db, test_application, test_team):
    application_list = Application.get_multi_by_team(test_team.id)
    assert application_list == [test_application]


def test_create_application(db, zk, test_team, faker):
    application_name = faker.uuid4()[:8]
    stat = zk.exists('/huskar/service/%s' % application_name)
    assert stat is None

    application = Application.create(application_name, test_team.id)
    assert zk.exists('/huskar/service/%s' % application_name)

    assert application.id > 0
    assert application.application_name == application_name
    assert application.domain_name == application_name
    assert application.team_id == test_team.id
    assert application.team.team_name == test_team.team_name

    user = User.get_by_name(application_name)
    assert user is not None
    assert user.is_application
    assert not user.is_admin
    assert application.check_auth(Authority.WRITE, user.id)
    assert application.check_auth(Authority.READ, user.id)

    with raises(NameOccupiedError):
        Application.create(application_name, test_team.id)  # name conflicts

    application = Application.create('baz', test_team.id)
    assert application.application_name == 'baz'


def test_create_application_setup_default_zpath_oos(
        mocker, zk, test_team, faker):
    application_name = faker.uuid4()[:8]
    mocker.patch.object(ServiceInfo, 'save', side_effect=OutOfSyncError())

    application = Application.create(application_name, test_team.id)
    assert application.id > 0
    stat = zk.exists('/huskar/service/%s' % application_name)
    assert stat is None


def test_create_application_setup_default_zpath_bypass(
        mocker, zk, test_team, faker):
    application_name = faker.uuid4()[:8]
    path = '/huskar/service/%s' % application_name
    zk.create(path, b'{}', makepath=True)

    application = Application.create(application_name, test_team.id)
    assert application.id > 0
    data, _ = zk.get(path)
    assert data == b'{}'


def test_create_application_cache_invalidation(db, test_team):
    assert Application.get_by_name('bar') is None
    assert len(Application.get_multi_by_team(test_team.id)) == 0
    assert len(Application.get_all()) == 0

    Application.create('bar', test_team.id)

    assert Application.get_by_name('bar') is not None
    assert len(Application.get_multi_by_team(test_team.id)) == 1
    assert len(Application.get_all()) == 1


def test_create_application_but_name_occupied(db, test_user, test_team):
    with raises(NameOccupiedError):
        Application.create(test_user.username, test_team.id)
    db.refresh(test_user)
    assert not test_user.is_application


def test_delete_application(
        db, test_user, test_team, test_application, test_application_auth):
    assert Application.get_by_name(
        test_application.application_name) is test_application
    assert Application.get_multi_by_team(test_team.id) == [test_application]
    assert Application.get_all() == [test_application]

    Application.delete(test_application.id)

    assert Application.get_by_name(test_application.application_name) is None
    assert Application.get_multi_by_team(test_team.id) == []
    assert Application.get_all() == []
    assert ApplicationAuth.get(test_application_auth.id) is None


def test_archive_application(
        db, test_user, test_team, test_application, test_application_auth):
    instance = Application.get_by_name(test_application.application_name)
    assert instance is test_application
    assert Application.get_multi_by_team(test_team.id) == [test_application]
    assert Application.get_all() == [test_application]

    test_application.archive()

    assert Application.get_by_name(test_application.application_name) is None
    assert Application.get_multi_by_team(test_team.id) == []
    assert Application.get_all() == []
    assert ApplicationAuth.find(Authority.READ, test_user.id,
                                test_application.id) is not None


def test_unarchive_application(
        db, test_user, test_team, test_application, test_application_auth):
    test_application.archive()
    assert Application.get_by_name(test_application.application_name) is None
    test_application.unarchive()
    assert Application.get_by_name(
        test_application.application_name) is not None

    test_application.archive()
    test_team.archive()
    test_application.unarchive()
    assert Application.get_by_name(test_application.application_name) is None


def test_application_transfer_team(db, test_application, faker):
    orig_team_id = test_application.team_id
    dest_team = Team.create(faker.name())
    test_application.transfer_team(dest_team.id)
    application = Application.get_by_name(test_application.application_name)
    assert application.team_id == dest_team.id
    assert test_application.id not in Application.get_ids_by_team(orig_team_id)
    assert test_application.id in Application.get_ids_by_team(dest_team.id)
