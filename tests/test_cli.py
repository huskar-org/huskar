from __future__ import absolute_import

import pytest

import huskar_api.cli
from huskar_api.models.auth import User


@pytest.fixture(scope='function')
def admin_user(db):
    admin_user = User.create_normal('admin', password='admin', is_active=True)
    admin_user.grant_admin()
    return admin_user


def test_cli_entry(mocker):
    run = mocker.patch.object(huskar_api.cli.manager, 'run')
    huskar_api.cli.main()
    run.assert_called_once()


def test_initdb_with_admin_present(mocker, admin_user):
    prompt_pass = mocker.patch.object(huskar_api.cli, 'prompt_pass')

    with pytest.raises(SystemExit):
        huskar_api.cli.initadmin()

    prompt_pass.assert_not_called()

    user = User.get_by_name('admin')
    assert user is admin_user


def test_initdb(mocker):
    prompt_pass = mocker.patch.object(huskar_api.cli, 'prompt_pass')
    prompt_pass.side_effect = ['', __name__]
    create_user = mocker.spy(User, 'create_normal')

    try:
        huskar_api.cli.initadmin()
    except SystemExit:
        pytest.fail('unexpected sys.exit')

    assert len(prompt_pass.mock_calls) == 2
    assert len(create_user.mock_calls) == 1

    user = User.get_by_name('admin')
    assert user is not None
    assert user.check_password(__name__)
    assert user.is_admin
