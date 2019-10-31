from __future__ import absolute_import

import gevent
import pytest
from sqlalchemy.orm import Session
from sqlalchemy.orm.scoping import ThreadLocalRegistry

from huskar_api import settings
from huskar_api.models import DBSession
import huskar_api.models.db as core_db
from huskar_api.models.db import make_session, session_stack, DBManager
from huskar_api.models.auth.team import Team


engines = {
    role: DBManager.create_engine(
        dsn,
        pool_size=10,
        max_overflow=1,
        pool_recycle=300,
        execution_options={'role': role})
    for role, dsn in settings.DB_SETTINGS['default']['urls'].iteritems()
}


@pytest.fixture
def team_point():
    team_point = Team(team_name='233')
    try:
        DBSession().query(Team).delete()
        DBSession().commit()
        DBSession().add(team_point)
        DBSession().commit()
        yield team_point
    finally:
        DBSession().query(Team).delete()
        DBSession().commit()


@pytest.fixture
def db_manager():
    try:
        yield core_db.db_manager
    finally:
        core_db.db_manager.close_sessions()
        core_db.db_manager.session_map.clear()


@pytest.fixture
def get_db_url(db_manager):
    def get_db_url(name, bind):
        session_factory = db_manager.session_map[name].session_factory
        return str(session_factory.kw['engines'][bind].url)
    return get_db_url


def test_create_sessions(mocker, db_manager, get_db_url):
    mocker.patch.object(settings, 'DB_SETTINGS', {
        'test': {
            'urls': {
                'master': 'mysql+pymysql://foo',
                'slave': 'mysql+pymysql://foo',
            },
        },
        'alternative': {
            'urls': {
                'master': 'mysql+pymysql://bar',
                'slave': 'mysql+pymysql://baz',
            },
        },
    })
    db_manager.create_sessions()
    assert sorted(db_manager.session_map) == ['alternative', 'default', 'test']

    assert get_db_url('test', 'master') == 'mysql+pymysql://foo'
    assert get_db_url('test', 'slave') == 'mysql+pymysql://foo'
    assert get_db_url('alternative', 'master') == 'mysql+pymysql://bar'
    assert get_db_url('alternative', 'slave') == 'mysql+pymysql://baz'


def test_create_sessions_with_infra_options(db_manager, mocker):
    mocker.patch.object(settings, 'DB_SETTINGS', {
        'test_23': {
            'urls': {
                'master': 'mysql+pymysql://foo',
                'slave': 'mysql+pymysql://foo',
                'max_overflow': 233,
                'pool_size': 666,
            },
        },
    })
    mocker.patch.object(db_manager, 'add_session', mocker.MagicMock())
    db_manager.create_sessions()
    assert (
        db_manager.add_session.call_args ==
        mocker.call('test_23', {
                'urls': {
                    'slave': 'mysql+pymysql://foo',
                    'master': 'mysql+pymysql://foo',
                    'pool_size': 666, 'max_overflow': 233,
                },
            }))


def test_session_stack():
    core_db.RAISE_CLOSING_EXCEPTION = True

    DBSession = make_session(engines, force_scope=True)

    session1 = DBSession()
    with session_stack():
        session2 = DBSession()
        session2.close()
        with session_stack():
            session3 = DBSession()
            session3.close()
    session1.close()

    assert not (session1 is session2 is session3)


def _invalidate_connections(session):
    for e in session.engines.values():
        for p in session.transaction._iterate_parents():
            conn = p._connections.get(e)
            if conn:
                conn[0].invalidate()


def test_with_statement_on_exc():
    class ThisExc(Exception):
        pass

    trans_orig = DBSession().transaction

    with pytest.raises(ThisExc):
        with DBSession() as session:
            session.execute('select 1')
            _invalidate_connections(session)
            raise ThisExc('boom!')

    session = DBSession()
    assert session.transaction.is_active
    assert session.transaction is not trans_orig
    session.commit()
    session.close()


def test_with_statement_closing(team_point):
    def get_team_point():
        return DBSession().query(team_point.__class__).get(team_point.id)

    assert DBSession().identity_map

    with DBSession().close_on_exit(False):
        pass
    assert DBSession().identity_map
    assert get_team_point() is team_point

    with DBSession():
        pass
    assert not DBSession().identity_map
    assert get_team_point() is not team_point


def test_upsert(team_point, mocker):
    session = DBSession()

    assert session.query(team_point.__class__.id).all() == [
        (team_point.id,),
    ]

    session.execute(core_db.Upsert(team_point.__table__).values(
        id=team_point.id, team_name='666'))

    assert session.query(team_point.__class__.id).all() == [
        (team_point.id,),
    ]

    session.execute(core_db.Upsert(team_point.__table__).values(
        id=team_point.id + 1, team_name='888'))

    assert session.query(team_point.__class__.id).all() == [
        (team_point.id,),
        (team_point.id + 1,),
    ]

    columns = Team.__table__.primary_key.columns

    class Mocked(object):
        def __len__(self):
            return 0

        def __getattr__(self, item):
            return getattr(columns, item)

    with mocker.patch.object(
        Team.__table__.primary_key, 'columns',
            mocker.MagicMock(return_value=Mocked)):
        session.execute(core_db.Upsert(team_point.__table__).values(
            id=team_point.id + 2, team_name='999'))


def test_upsert_mixin(mocker):
    class Foo(core_db.UpsertMixin):
        __table__ = mocker.Mock()

    stmt = Foo.upsert()
    assert stmt.table is Foo.__table__._sa_instance_state.selectable


def test_comfirm_close_when_exception(mocker):
    with mocker.patch.object(
            Session, 'commit', mocker.MagicMock(
            side_effect=[gevent.Timeout(), ])):
        session = DBSession()
        session.execute("select 1")
        session.flush()
        with pytest.raises(gevent.Timeout):
            session.commit()
        for engine in session.engines.itervalues():
            for parent in session.transaction._iterate_parents():
                conn = parent._connections.get(engine)
                if conn:
                    assert conn[0].invalidated
        session.close()


def test_db_manager_error_missing_settings(mocker):
    with mocker.patch.object(settings, 'DB_SETTINGS',
                             mocker.MagicMock(
                                 __nonzero__=mocker.MagicMock(
                                     return_value=False))):
        with pytest.raises(ValueError):
            DBManager()


def test_db_manager_error_dup_create(mocker):
    db = DBManager()

    with pytest.raises(ValueError):
        db.create_sessions()

    assert core_db.close_connections(None, None) is None

    with mocker.patch.object(settings, 'IS_IN_DEV',
                             mocker.MagicMock(
                                 __nonzero__=mocker.MagicMock(
                                     return_value=False))):
        db = DBManager()
        session = db.session_map['default']
        assert isinstance(session.registry, ThreadLocalRegistry)


def test_other_error():
    c = core_db.RecycleField()
    with pytest.raises(AttributeError):
        c.test

    with pytest.raises(KeyError):
        session = DBManager()
        session.get_session('233_not_exists')


def test_close_sessions_extra():
    session = DBManager()
    session.close_sessions(should_close_connection=False)
    session.close_sessions(should_close_connection=True)

    db_manager = DBManager()
    DBSession = db_manager.get_session('default')

    class ThisExc(Exception):
        pass

    with pytest.raises(ThisExc):
        with DBSession() as session:
            session.execute('select 1')
            _invalidate_connections(session)
            raise ThisExc('boom!')

    session = DBSession()
    assert session.transaction.is_active
    session.transaction = None
    db_manager.close_sessions(should_close_connection=True)
