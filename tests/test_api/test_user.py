from __future__ import absolute_import

import uuid
import datetime

from pytest import mark, fixture
from pytz import utc

from huskar_api.models.audit import AuditLog
from huskar_api.models.auth import User
from huskar_api.models.signals import new_action_detected
from huskar_api.extras.email import EmailTemplate
from ..utils import assert_response_ok


@fixture(autouse=True)
def tzlocal(mocker):
    return mocker.patch(
        'huskar_api.extras.marshmallow.tzlocal', return_value=utc)


@fixture
def add_test_user(db):
    def factory(fields):
        db.execute(User.__table__.insert().values(**fields))
        db.commit()
    return factory


@mark.xparametrize
def test_add_user(
        client, admin_token, db, ceiled_now, add_test_user, last_audit_log,
        presented_data, input_data, expected_data):
    for fields in presented_data:
        add_test_user(fields)

    headers = {'Authorization': admin_token}
    r = client.post('/api/user', data=input_data, headers=headers)

    assert_response_ok(r)
    assert r.json['data'] is None

    result_set = db.execute(
        'select * from user where username = :username', input_data)
    assert result_set.rowcount == 1

    row = result_set.fetchone()
    for expected_key, expected_value in expected_data.items():
        assert row[expected_key] == expected_value
    assert row['created_at'] and row['created_at'] <= ceiled_now

    audit_log = last_audit_log()
    assert audit_log.action_name == 'CREATE_USER'
    assert audit_log.action_json['username'] == input_data['username']


def test_recreate_archived_user(client, admin_token, add_test_user):
    user_data = {
        'username': 'foo',
        'password': 'bar',
        'email': 'foo@test.me'
    }
    headers = {'Authorization': admin_token}

    add_test_user(user_data)
    user = User.get_by_name('foo')
    user.archive()

    r = client.post('/api/user', data=user_data, headers=headers)

    assert r.status_code == 400
    assert r.json['message']


@mark.xparametrize
def test_add_user_failed(client, admin_token, db, ceiled_now, last_audit_log,
                         add_test_user, test_application_token,
                         presented_data, input_token, input_data,
                         expected_code, expected_data):
    for fields in presented_data:
        add_test_user(fields)

    if input_token == '%test_application_token%':
        headers = {'Authorization': test_application_token}
    else:
        headers = {'Authorization': input_token or admin_token}
    r = client.post('/api/user', data=input_data, headers=headers)

    assert r.status_code == expected_code, r.data
    assert r.json == expected_data

    result_set = db.execute('select * from user')
    assert result_set.rowcount == len(presented_data) + len([
        admin_token, test_application_token])

    result_set = db.execute(
        'select * from user where email = :email', input_data)
    assert result_set.rowcount == 0, 'this user should not be added'

    assert last_audit_log() is None


@mark.parametrize('username,email', [
    ('san.zhang@foo.bar', 'test'),
    ('san.zhang', 'san.zhang@foo.bar@foo.bar')
])
def test_add_user_with_invalid_data(client, admin_token, username, email):
    add_test_user({
        'username': 'san.zhang',
        'email': 'san.zhang@foo.bar',
        'password': 'test'
    })
    headers = {'Authorization': admin_token}
    r = client.post('/api/user', data={'username': username, 'email': email},
                    headers=headers)
    assert r.status_code == 400
    assert r.json['status'] == 'ValidationError'


@mark.xparametrize
def test_get_user(client, admin_token, add_test_user,
                  presented_data, input_data, expected_data):
    for fields in presented_data:
        add_test_user(fields)

    headers = {'Authorization': admin_token}
    r = client.get('/api/user/%(username)s' % input_data, headers=headers)
    assert_response_ok(r)
    for expected_key, expected_value in expected_data.items():
        assert r.json['data'][expected_key] == expected_value


@mark.xparametrize
def test_get_user_list(client, admin_token, add_test_user,
                       presented_data, expected_data):
    for fields in presented_data:
        add_test_user(fields)

    headers = {'Authorization': admin_token}
    r = client.get('/api/user', headers=headers)
    assert_response_ok(r)
    assert r.json['data']

    response_data = {d['username']: d for d in r.json['data']}
    assert response_data.pop('admin', None)
    assert len(response_data) == len(expected_data)
    for fields in expected_data:
        item = response_data[fields['username']]
        assert item['id'] > 0
        for expected_key, expected_value in fields.items():
            assert item[expected_key] == expected_value


@mark.xparametrize
def test_delete_user(client, db, admin_token, add_test_user, last_audit_log,
                     presented_data, input_data, expected_data):
    for fields in presented_data:
        add_test_user(fields)

    headers = {'Authorization': admin_token}
    r = client.delete('/api/user/%(username)s' % input_data, headers=headers)
    assert_response_ok(r)
    assert r.json['data'] is None

    result_set = db.execute('select * from user where is_active = 1')
    assert result_set.rowcount - 1 == len(expected_data)  # except 'admin'
    for fields in expected_data:
        row = result_set.fetchone()
        if row['username'] == 'admin':
            continue
        for expected_key, expected_value in fields.items():
            assert row[expected_key] == expected_value

    audit_log = last_audit_log()
    assert audit_log.action_name == 'ARCHIVE_USER'
    assert audit_log.action_json['username'] == input_data['username']


@mark.xparametrize
def test_delete_user_failed(client, db, admin_token, last_audit_log,
                            add_test_user, test_application_token,
                            presented_data, input_token, input_data,
                            expected_code, expected_data):
    for fields in presented_data:
        add_test_user(fields)

    if input_token == '%test_application_token%':
        headers = {'Authorization': test_application_token}
    else:
        headers = {'Authorization': input_token or admin_token}
    r = client.delete('/api/user/%(username)s' % input_data, headers=headers)
    assert r.status_code == expected_code, r.data
    assert r.json == expected_data

    result_set = db.execute('select * from user')
    assert result_set.rowcount == len(presented_data) + len([
        admin_token, test_application_token])

    assert last_audit_log() is None


@mark.xparametrize
def test_change_password(client, db, last_audit_log, add_test_user,
                         presented_data, input_token, input_data,
                         expected_data, expected_password):
    for fields in presented_data:
        add_test_user(fields)

    username = input_data.pop('username')

    url = '/api/user/%s' % username
    headers = {'Authorization': input_token}
    r = client.put(url, data=input_data, headers=headers)

    assert_response_ok(r)
    assert r.json == expected_data

    result_set = db.execute(
        'select password from user where username = :username',
        {'username': username})
    assert result_set.scalar() == expected_password

    audit_log = last_audit_log()
    assert audit_log.action_name == 'CHANGE_USER_PASSWORD'
    assert audit_log.action_json['username'] == username


@mark.xparametrize
def test_change_password_failed(client, db, last_audit_log, add_test_user,
                                presented_data, input_token, input_data,
                                expected_code, expected_data):
    for fields in presented_data:
        add_test_user(fields)

    username = input_data.pop('username')
    result_set = db.execute(
        'select password from user where username = :username',
        {'username': username})
    presented_password = result_set.scalar()

    url = '/api/user/%s' % username
    headers = {'Authorization': input_token}
    r = client.put(url, data=input_data, headers=headers)

    assert r.status_code == expected_code, r.data
    assert r.json == expected_data

    result_set = db.execute(
        'select password from user where username = :username',
        {'username': username})
    assert result_set.scalar() == presented_password

    assert last_audit_log() is None


@mark.xparametrize
def test_change_email(client, db, add_test_user,
                      presented_data, input_token, input_data,
                      expected_data, expected_password, expected_email):
    for fields in presented_data:
        add_test_user(fields)

    username = input_data.pop('username')

    url = '/api/user/%s' % username
    headers = {'Authorization': input_token}
    r = client.put(url, data=input_data, headers=headers)

    assert_response_ok(r)
    assert r.json == expected_data

    result_set = db.execute(
        'select password, email from user where username = :username',
        {'username': username})
    assert result_set.rowcount == 1

    row = result_set.fetchone()
    assert row['password'] == expected_password
    assert row['email'] == expected_email


@mark.xparametrize
def test_password_reset(client, db, add_test_user, mocker, last_audit_log,
                        presented_data, input_data, generated_uuid,
                        expected_request_error, expected_status_code,
                        expected_response, expected_password):
    add_test_user(presented_data)

    uuid4 = mocker.patch('uuid.uuid4')
    uuid4.return_value = uuid.UUID(generated_uuid)
    deliver_email = mocker.patch('huskar_api.service.admin.user.deliver_email')
    recorded = []

    @new_action_detected.connect_via(AuditLog)
    def record(sender, **kwargs):
        recorded.append(kwargs)

    url = '/api/user/{username}/password-reset'.format(
        username=input_data['username'])
    r = client.post(url)

    if expected_request_error:
        assert r.status_code == expected_request_error.pop('code', None)
        assert r.json == expected_request_error
        return

    assert_response_ok(r)
    assert r.json['data'] == {'email': presented_data['email']}

    audit_log = last_audit_log()
    assert audit_log.action_name == 'FORGOT_USER_PASSWORD'
    assert audit_log.action_json['username'] == input_data['username']
    assert len(recorded) == 1
    assert recorded[0]['username'] is None

    deliver_email.assert_called_once_with(
        EmailTemplate.PASSWORD_RESET, presented_data['email'], {
            'username': input_data['username'],
            'token': uuid4(),
            'expires_in': datetime.timedelta(minutes=10),
        },
    )

    db.close()

    r = client.post(url, data={
        'token': input_data['token'], 'password': input_data['password']})
    assert r.status_code == expected_status_code
    assert r.json == expected_response

    password = db.execute(
        'select password from user where username = :username',
        {'username': input_data['username']}).scalar()
    assert password == expected_password

    if expected_status_code == 200:
        audit_log = last_audit_log()
        assert audit_log.action_name == 'CHANGE_USER_PASSWORD'
        assert audit_log.action_json['username'] == input_data['username']


def test_password_reset_on_application_user(
        client, mocker, db, test_application):
    user = test_application.setup_default_auth()
    password = user.password
    token = '2941dabbedd54c0ab04446ff3260c21f'

    redis_client = mocker.patch('huskar_api.service.admin.user._redis_client')
    redis_client.get.return_value = token

    url = '/api/user/%s/password-reset' % user.username
    r = client.post(url, data={'token': token, 'password': 'new-password'})
    assert r.status_code == 404
    assert r.json['status'] == 'NotFound'
    assert r.json['message'] == 'user %s not found' % user.username

    present_password = db.execute(
        'select password from user where username = :username',
        {'username': user.username}).scalar()
    assert present_password == password
