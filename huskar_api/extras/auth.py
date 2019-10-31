from __future__ import absolute_import

import collections
import logging

from werkzeug.security import gen_salt

from huskar_api.models.auth import Application, User, Team, Authority
from huskar_api.models.exceptions import NameOccupiedError
from huskar_api.extras.email import (
    deliver_email, EmailTemplate, EmailDeliveryError)
from huskar_api.models.utils import retry
from huskar_api.models.audit import action_types
from .utils import huskar_audit_log


logger = logging.getLogger(__name__)


USER_ROLE_MAP = {
    'owner': 'write',
    'writer': 'write',
    'reader': 'read'
}


def ensure_owners(app_info):
    for owner in app_info.owners:
        authority = Authority(USER_ROLE_MAP[owner.role])
        try:
            user = owner.ensure()
        except NameOccupiedError:
            logger.info('Username %s was occupied.', owner.name)
        except EmailDeliveryError:
            logger.warn('Send mail to %s failed.', owner.email)
        else:
            yield user, authority


def authorize_owners(app_info):
    """Grants the authority of application owners.

    The application owners should be authorized to write his (her)
    applications.
    """
    application = app_info.ensure_application()
    now_auth = set([(i.user_id, Authority(i.authority))
                    for i in application.list_auth()])
    need_ensure_owners = set([(user.id, authority)
                              for (user, authority) in ensure_owners(
                                app_info)])
    for user_id, authority in need_ensure_owners.difference(now_auth):
        user = User.get(user_id)
        with huskar_audit_log(action_types.GRANT_APPLICATION_AUTH,
                              user=user, application=application,
                              authority=authority.value):
            application.ensure_auth(authority, user_id)
        logger.info(
            'granting user "%s" to %s application "%s"',
            user.username, authority, application.application_name)


class Department(collections.namedtuple('Department', [
    'parent_id',
    'parent_name',
    'child_id',
    'child_name',
])):
    UNKNOWN_CHILD_ID = 0
    UNKNOWN_CHILD_NAME = 'unknown'

    @property
    def team_name(self):
        if self.parent_id:
            return u'{}-{}'.format(self.parent_id, self.child_id)

        return unicode(self.child_id)

    @property
    def team_desc(self):
        if self.parent_name:
            return u'{}-{}'.format(self.parent_name, self.child_name)

        return self.child_name


class AppInfo(collections.namedtuple('AppInfo', [
    'department',
    'application_name',
    'owners',
])):
    PROCESSORS = [authorize_owners]

    @classmethod
    def from_external(cls, team_name, application_name,
                      owner_name, owner_email):
        department = Department(
            None, None, team_name.lower(), team_name.lower())
        owner = Owner(owner_name, owner_email, Owner.ROLE_OWNER)
        return AppInfo(department, application_name, [owner])

    @retry(NameOccupiedError, 1, 2)
    def ensure_team(self):
        return Team.get_by_name(self.department.team_name) or Team.create(
            self.department.team_name, self.department.team_desc)

    @retry(NameOccupiedError, 1, 2)
    def ensure_application(self):
        return (
            Application.get_by_name(self.application_name) or
            Application.create(self.application_name, self.ensure_team().id))

    @retry(NameOccupiedError, 1, 2)
    def ensure_application_user(self):
        return self.ensure_application().setup_default_auth()

    def submit_to_import(self):
        for processor in self.PROCESSORS:
            processor(self)

        return self.ensure_application(), self.ensure_application_user()


class Owner(collections.namedtuple('Owner', ['name', 'email', 'role'])):
    ROLE_OWNER = 'owner'

    @retry(NameOccupiedError, 1, 2)
    def ensure(self):
        user = (User.get_by_name(self.name) or
                User.get_by_email(self.email))
        if user is None:
            password = gen_salt(30)
            user = User.create_normal(
                self.name, password, self.email, is_active=True)
            deliver_email(EmailTemplate.SIGNUP, user.email, {
                'username': user.username,
                'password': password,
            })
        return user
