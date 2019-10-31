from __future__ import absolute_import
import datetime
import os
import uuid

import py.path
from flask import render_template
import pytest

from huskar_api import settings
from huskar_api.app import create_app
from huskar_api.extras.email import (
    EmailTemplate, render_email_template, deliver_email, AbstractMailClient)

DOCUMENT_DIR = py.path.local(__file__).dirpath('../../docs/assets')
SNAPSHOT_DIR = py.path.local(__file__).dirpath('test_email_snapshots')
SNAPSHOT_ARGUMENTS = [
    (0, EmailTemplate.DEBUG, {'foo': 'bar'}),
    (0, EmailTemplate.SIGNUP, {
        'username': 'san.zhang',
        'password': '123456',
    }),
    (0, EmailTemplate.PASSWORD_RESET, {
        'username': 'san.zhang',
        'token': uuid.UUID('063ac20c-7be8-4d3c-92e1-e6e503169c20'),
        'expires_in': datetime.timedelta(minutes=1),
    }),
    (0, EmailTemplate.PERMISSION_GRANT, {
        'username': 'san.zhang',
        'application_name': 'base.foo',
        'authority': 'write',
    }),
    (0, EmailTemplate.PERMISSION_DISMISS, {
        'username': 'san.zhang',
        'application_name': 'base.bar',
        'authority': 'admin',
    }),
    (0, EmailTemplate.INFRA_CONFIG_CREATE, {
        'infra_name': 'book-postgres',
        'infra_type': 'database',
        'application_name': 'foo.test',
        'is_authorized': True,
    }),
    (1, EmailTemplate.INFRA_CONFIG_CREATE, {
        'infra_name': 'book-redis',
        'infra_type': 'redis',
        'application_name': 'foo.test',
        'is_authorized': True,
    }),
    (2, EmailTemplate.INFRA_CONFIG_CREATE, {
        'infra_name': 'book-amqp',
        'infra_type': 'amqp',
        'application_name': 'foo.test',
        'is_authorized': True,
    }),
    (3, EmailTemplate.INFRA_CONFIG_CREATE, {
        'infra_name': 'book-amqp',
        'infra_type': 'amqp',
        'application_name': 'foo.test',
        'is_authorized': False,
    }),
    (4, EmailTemplate.INFRA_CONFIG_CREATE, {
        'infra_name': 'book-es',
        'infra_type': 'es',
        'application_name': 'foo.test',
        'is_authorized': False,
    }),
    (5, EmailTemplate.INFRA_CONFIG_CREATE, {
        'infra_name': 'book-oss',
        'infra_type': 'oss',
        'application_name': 'foo.test',
        'is_authorized': False,
    }),
    (6, EmailTemplate.INFRA_CONFIG_CREATE, {
        'infra_name': 'book-kafka',
        'infra_type': 'kafka',
        'application_name': 'foo.test',
        'is_authorized': False,
    }),
]

SNAPSHOT_ANNOUNCE = '''
OPEN THEM IN YOUR BROWSER. Don't forget to review the generated snapshots
before committing them.

    open {dir}/*.html
'''.strip().format(dir=SNAPSHOT_DIR)

SNAPSHOT_TEST_FAILED = '''
----------------------------
The snapshot test is failed.
----------------------------

Re-generate snapshots if you changed any email template before:

    python run.py -i tests.test_extras.test_email:gen

{announce}
'''.rstrip().format(announce=SNAPSHOT_ANNOUNCE)


@pytest.mark.parametrize('id_,template,arguments', SNAPSHOT_ARGUMENTS)
def test_email_templates(client, id_, template, arguments):
    filename, _, required_arguments = template.value
    snapshot = get_snapshot(filename, id_).read().decode('utf-8')
    result = render_email_template(template, **arguments)
    assert set(arguments) == required_arguments
    assert snapshot == result, SNAPSHOT_TEST_FAILED


def get_snapshot(filename, id_):
    root, ext = os.path.splitext(filename)
    filename_with_id = '%s-%d%s' % (root, id_, ext)
    return SNAPSHOT_DIR.join(filename_with_id)


def gen():
    """Generates snapshot with updated template."""
    # We can not use pytest fixtures without pytest as launcher.
    # Is there a better way?
    settings.ADMIN_HOME_URL = 'http://example.com'
    settings.ADMIN_SIGNUP_URL = 'http://example.com/ewf'
    settings.ADMIN_RESET_PASSWORD_URL = \
        'http://example.com/password-reset/{username}/{token}'
    settings.ADMIN_INFRA_CONFIG_URL = \
        'http://example.com/application/{application_name}/config?' \
        'infra_type={infra_type}&infra_name={infra_name}'
    app = create_app()

    print('-' * 70)
    print('Generating snapshots ...')
    print('-' * 70)
    indices = []
    for id_, template, arguments in SNAPSHOT_ARGUMENTS:
        filename, _, _ = template.value
        with app.app_context():
            result = render_email_template(template, **arguments)
        with get_snapshot(filename, id_).open('w') as snapshot:
            snapshot.write(result.encode('utf-8'))
        print('%s %r\n%s\n---' % (template.name, arguments, snapshot.name))
        relpath = DOCUMENT_DIR.bestrelpath(get_snapshot(filename, id_))
        indices.append((id_, template.value, relpath))
    print('Generating documents ...')
    with app.app_context():
        result = render_template('docs-assets-index.rst', indices=indices)
        with DOCUMENT_DIR.join('index.rst').open('w') as index:
            index.write('.. DO NOT EDIT (auto generated)\n\n')
            index.write(result.encode('utf-8'))

    print('-' * 70)
    print(SNAPSHOT_ANNOUNCE)
    print('-' * 70)


def test_deliver_email_value_error():
    t = EmailTemplate.DEBUG

    with pytest.raises(ValueError):
        deliver_email(t, 'a@example.com', {})


def test_deliver_email_with_client(mocker, app):

    class Client(AbstractMailClient):
        def __init__(self):
            self.s = {}

        def deliver_email(self, receiver, subject, message, cc):
            self.s.update(receiver=receiver, subject=subject,
                          message=message, cc=cc)

    t = EmailTemplate.DEBUG
    cc = ['a@a.com', 'b@b.com']
    receiver = 'a@example.com'
    c = Client()

    deliver_email(t, receiver, {'foo': 'bar'}, cc, client=c)
    assert c.s['receiver'] == receiver
    assert c.s['subject'] == t.value[1]
    assert c.s['cc'] == cc


if __name__ == '__main__':
    gen()
