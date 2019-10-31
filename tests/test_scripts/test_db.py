from __future__ import absolute_import

from pytest import fixture, raises

import huskar_api.scripts.db


@fixture(scope='function')
def get_engine(mocker, monkeypatch, db):
    engine = db.engines['master']
    new_database = '%s_test_db_script' % engine.url.database
    monkeypatch.setattr(engine.url, 'database', new_database)
    return mocker.spy(huskar_api.scripts.db, 'get_engine')


def test_initdb(db, get_engine):
    db.close()

    assert get_engine.call_count == 0

    with db.close_on_exit(True):
        before_tables = db.execute('show tables').fetchall()

    huskar_api.scripts.db.initdb()

    with db.close_on_exit(True):
        after_tables = db.execute('show tables').fetchall()

    assert before_tables == after_tables
    assert get_engine.call_count > 0


def test_initdb_in_production(get_engine, mocker):
    mocker.patch('huskar_api.settings.IS_IN_DEV', False)

    with raises(RuntimeError):
        huskar_api.scripts.db.initdb()
    assert get_engine.call_count == 0


def test_dumpdb(db, mocker):
    db.close()

    schema_open = mocker.patch('huskar_api.scripts.db.open')
    schema_file = schema_open()
    schema_file.__enter__ = mocker.Mock()
    schema_file.__enter__.return_value = schema_file
    schema_file.__exit__ = mocker.Mock()

    huskar_api.scripts.db.dumpdb()

    schema_file.writelines.assert_called_once()
