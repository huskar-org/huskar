from __future__ import absolute_import

from flask import request, abort, g
from flask.views import MethodView

from huskar_api import settings
from huskar_api.models.auth import (
    User, Team, Application, SessionAuth,
    Authority)
from huskar_api.extras.email import EmailTemplate
from huskar_api.service.admin.exc import (
    LoginError, UserNotExistedError, AuthorityExistedError,
    AuthorityNotExistedError, NoAuthError)
from .schema import application_auth_schema
from .utils import (
    login_required, api_response, minimal_mode_incompatible, audit_log,
    deliver_email_safe)


class HuskarAdminView(MethodView):

    @login_required
    @minimal_mode_incompatible
    def post(self):
        """Grants an user to site admin.

        :form username: The username of granted user.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The user is not found.
        :status 200: The user is a site admin already.
        :status 201: The user has been granted successfully.
        """
        g.auth.require_admin('only admin can add huskar admin')

        user = self._get_user_or_404(request.form['username'])
        if user and user.is_admin:
            return api_response(), 200
        with audit_log(audit_log.types.GRANT_HUSKAR_ADMIN, user=user):
            user.grant_admin()
        return api_response(), 201

    @login_required
    @minimal_mode_incompatible
    def delete(self, username):
        """Dismisses an user from site admin.

        :form username: The username of dismissed user.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 403: The dismissing is not permitted.
        :status 404: The user is not found.
        :status 200: The user has been dismissed successfully.
        """
        g.auth.require_admin('only admin can delete admin')

        user = self._get_user_or_404(username)
        if user.username == g.auth.username:
            abort(403, 'It is not allowed to dismiss yourself')
        with audit_log(audit_log.types.DISMISS_HUSKAR_ADMIN, user=user):
            user.dismiss_admin()
        return api_response()

    def _get_user_or_404(self, username):
        user = User.get_by_name(username)
        if user is None:
            abort(404, 'user "%s" is not found' % username)
        return user


class ApplicationAuthView(MethodView):
    @login_required
    @minimal_mode_incompatible
    def get(self, application_name):
        """Gets the authority list of current application.

        The response looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": {
                "app_authority": [
                  {"username": "base.foo", "authority": "read"},
                ],
                "user_authority": [
                  {"username": "san.zhang", "authority": "write"},
                ]
              }
            }

        :param application_name: The name of application.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application with specified name is not found.
        :status 200: The authority list is in the response.
        """
        application = self._get_application_or_404(application_name)
        auth_list = application.list_auth()
        auth_data = application_auth_schema.dump(auth_list, many=True).data
        return api_response({'application_auth': auth_data})

    @login_required
    @minimal_mode_incompatible
    def post(self, application_name):
        """Grants an user to have specified authority of current application.

        :param application_name: The name of application.
        :form authority: The authority name, ``read`` or ``write``.
        :form username: The username of granting one.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application with specified name is not found.
        :status 200: The authority is granted.
        """
        authority = request.form['authority'].strip()
        user = self._get_user_or_400(request.form['username'].strip())
        application = self._get_application_or_404(application_name)
        with audit_log(audit_log.types.GRANT_APPLICATION_AUTH,
                       user=user, application=application,
                       authority=authority):
            self._add_application_auth(application, user, authority)
        if (not (user and user.is_admin) and
                user.email):
            deliver_email_safe(EmailTemplate.PERMISSION_GRANT, user.email, {
                'username': user.username,
                'application_name': application.application_name,
                'authority': authority,
            })
        return api_response()

    @login_required
    @minimal_mode_incompatible
    def delete(self, application_name):
        """Dismisses an user from specified authority of current application.

        :param application_name: The name of application.
        :form authority: The authority name, ``read`` or ``write``.
        :form username: The username of dismissing one.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application with specified name is not found.
        :status 200: The authority is dismissed.
        """
        authority = request.form['authority'].strip()
        user = self._get_user_or_400(request.form['username'].strip())
        application = self._get_application_or_404(application_name)
        with audit_log(audit_log.types.DISMISS_APPLICATION_AUTH,
                       user=user, application=application,
                       authority=authority):
            self._delete_application_auth(application, user, authority)
        if (not (user and user.is_admin) and
                user.email):
            deliver_email_safe(EmailTemplate.PERMISSION_DISMISS, user.email, {
                'username': user.username,
                'application_name': application.application_name,
                'authority': authority,
            })
        return api_response()

    def _get_application_or_404(self, application_name):
        application = Application.get_by_name(application_name)
        if application is None:
            abort(404, 'application %s does not exist' % application_name)
        return application

    def _get_user_or_400(self, username):
        user = User.get_by_name(username)
        if user is None:
            abort(400, 'user %s does not exist' % username)
        return user

    def _add_application_auth(self, application, user, authority):
        self._check_application_admin(application)
        try:
            authority = Authority(authority)
        except ValueError:
            abort(400, 'authority must be one of "read", "write"')
        if application.check_auth(authority, user.id):
            raise AuthorityExistedError('this authority has existed')
        application.ensure_auth(authority, user.id)

    def _delete_application_auth(self, application, user, authority):
        self._check_application_admin(application)
        try:
            authority = Authority(authority)
        except ValueError:
            abort(400, 'authority must be one of "read", "write"')

        if not application.check_auth(authority, user.id):
            raise AuthorityNotExistedError("{} doesn't have {} of {}".format(
                user.username, authority.value, application.application_name))
        application.discard_auth(authority, user.id)

    def _check_application_admin(self, application):
        if application.check_auth(Authority.ADMIN, g.auth.id):
            return
        raise NoAuthError('{} has no admin authority on {}'.format(
            g.auth.username, application.application_name))


class HuskarTokenView(MethodView):
    @minimal_mode_incompatible
    def post(self):
        """Obtains an user token by checking username and password.

        :form username: Required. The username of user.
        :form password: Required. The password of user.
        :form expiration: Optional. The life time in seconds of the token.
                          ``0`` or omitted will make token be expired in 30
                          days. Default is ``0``.
        :<header Content-Type: :mimetype:`application/x-www-form-urlencoded`
        :status 400: The username or password is incorrect.
        :status 200: You could find token from the response body:
                     ``{"status": "SUCCESS", "data": {"token": "..",
                     "expires_in": 1}}``
        """
        username = request.form['username'].strip()
        password = request.form['password'].strip()
        expires_in = request.form.get('expiration', type=int, default=0)
        if expires_in > 0:
            expires_in = min(expires_in, settings.ADMIN_MAX_EXPIRATION)
        else:
            expires_in = settings.ADMIN_MAX_EXPIRATION

        user = User.get_by_name(username)
        if user is None or user.is_application:
            raise UserNotExistedError('user not found')
        if not user.check_password(password):
            raise LoginError("username or password not correct")

        g.auth = SessionAuth.from_user(user)
        with audit_log(audit_log.types.OBTAIN_USER_TOKEN, user=user):
            token = user.generate_token(settings.SECRET_KEY, expires_in)
            return api_response({'token': token, 'expires_in': expires_in})


class TeamAdminView(MethodView):
    @login_required
    @minimal_mode_incompatible
    def get(self, team_name):
        """Gets the admin list of specified team.

        The response is looks like::

            {"status": "SUCCESS", "message": "", "data": {"admin": ["san.zh"]}}

        :param team_name: The name of specified team.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The team with specified name is not found.
        :status 200: The list is in the response.
        """
        team = self._get_team_or_404(team_name)
        user_list = [user.username for user in team.list_admin()]
        return api_response({'admin': user_list})

    @login_required
    @minimal_mode_incompatible
    def post(self, team_name):
        """Grants an user to the admin of specified team.

        :param team_name: The name of specified team.
        :form username: The username of granting user.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The granting user is not found.
        :status 404: The team with specified name is not found.
        :status 200: The user is admin of this team already.
        :status 201: The user is granted successfully.
        """
        g.auth.require_admin('only huskar admin can add team admin')

        user_name = request.form['username'].strip()
        team = self._get_team_or_404(team_name)
        user = self._get_user_or_400(user_name)

        if team.check_auth(Authority.WRITE, user.id):
            return api_response()
        with audit_log(audit_log.types.GRANT_TEAM_ADMIN,
                       user=user, team=team):
            team.grant_admin(user.id)
        return api_response(), 201

    @login_required
    @minimal_mode_incompatible
    def delete(self, team_name):
        """Dismisses an user from the admin of specified team.

        :param team_name: The name of specified team.
        :form username: The username of dismissing user.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The user is not admin of specified team.
        :status 404: The team with specified name is not found.
        :status 200: The user is dismissed successfully.
        """
        g.auth.require_admin('only huskar admin can delete team admin')

        user_name = request.form['username'].strip()
        team = self._get_team_or_404(team_name)
        user = self._get_user_or_400(user_name)

        if not team.check_auth(Authority.WRITE, user.id):
            abort(400, '%s is not admin of %s' % (user_name, team_name))
        with audit_log(audit_log.types.DISMISS_TEAM_ADMIN,
                       user=user, team=team):
            team.dismiss_admin(user.id)
        return api_response()

    def _get_team_or_404(self, team_name):
        team = Team.get_by_name(team_name)
        if team is None:
            abort(404, 'team %s does not exist' % team_name)
        return team

    def _get_user_or_400(self, user_name):
        user = User.get_by_name(user_name)
        if user is None:
            abort(400, 'user %s does not exist' % user_name)
        return user
