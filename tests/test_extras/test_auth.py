from __future__ import absolute_import

from huskar_api.extras.email import EmailDeliveryError
from huskar_api.extras.auth import (
    ensure_owners, AppInfo, Department, Owner, NameOccupiedError)
from huskar_api.models.auth import User


def test_ensure_ensure_owners_send_mail_failed(mocker):
    deliver_email = mocker.patch('huskar_api.extras.auth.deliver_email')
    deliver_email.side_effect = EmailDeliveryError()

    prefix = 'test_ensure_ensure_owners_send_mail_failed'
    application_name = '{}_app'.format(prefix)
    owner = Owner(
            '{}_user'.format(prefix),
            '{}_user@a.com'.format(prefix),
            'owner',
        )
    department = Department(
            '1', '{}_team'.format(prefix), '2', '{}_team'.format(prefix))
    appinfo = AppInfo(
        department=department,
        application_name=application_name,
        owners=[owner])

    assert department.team_name == '{}-{}'.format(
        department.parent_id, department.child_id)
    assert department.team_desc == '{}-{}'.format(
        department.parent_name, department.child_name)

    assert len(list(ensure_owners(appinfo))) == 0

    assert User.get_by_name('{}_user'.format(prefix)) is not None
    deliver_email.assert_called_once()

    mocker.patch.object(owner, 'ensure',
                        mocker.MagicMock(side_effect=NameOccupiedError))
    assert len(list(ensure_owners(appinfo))) == 0


def test_department():
    parent_name = ''
    child_name = '233_team_child'
    department = Department(None, parent_name, 233, child_name)
    assert department.team_name == '233'
    assert department.team_desc == child_name
    assert department.parent_name == parent_name
