from __future__ import absolute_import

from copy import deepcopy
from subprocess import check_output

from pkg_resources import resource_string, resource_filename
from sqlalchemy.engine import create_engine
from huskar_api import settings
from huskar_api.models import DBSession


__all__ = ['initdb', 'dumpdb']


SCHEMA_FILE = ('huskar_api', '../database/mysql.sql')


def initdb():
    if not settings.IS_IN_DEV:
        raise RuntimeError('Should never use this in production environment')

    engine = get_engine()
    database_name = quote(engine.dialect, engine.url.database)
    schema_ddl = resource_string(*SCHEMA_FILE)

    anonymous_url = deepcopy(engine.url)
    anonymous_url.database = None
    anonymous_url.query = {}
    anonymous_engine = create_engine(anonymous_url)

    with anonymous_engine.connect() as connection:
        connection.execute(
            'drop database if exists {0}'.format(database_name))
        connection.execute(
            'create database {0} character set utf8mb4 '
            'collate utf8mb4_bin'.format(database_name))

    with anonymous_engine.connect() as connection:
        connection.execute('use {0}'.format(database_name))
        connection.execute(schema_ddl)


def dumpdb():
    ddl = mysqldump_output('--no-data')
    sql = mysqldump_output('--tables', 'alembic_version', '--no-create-info')
    with open(resource_filename(*SCHEMA_FILE), 'w') as schema_file:
        schema_file.writelines([ddl.strip(), '\n\n', sql.strip(), '\n'])


def get_engine():
    session = DBSession()
    return session.engines['master']


def mysqldump_output(*args):
    engine = get_engine()
    process_args = [
        'mysqldump',
        '--host=%s' % engine.url.host,
        '--port=%s' % engine.url.port,
        '--user=%s' % engine.url.username,
    ]
    if engine.url.password:  # pragma: no cover
        process_args.append('--password=%s' % engine.url.password)
    process_args.append(engine.url.database)
    process_args.extend(args)
    return check_output(process_args)


def quote(dialect, literal):
    return dialect.preparer(dialect).quote(literal)
