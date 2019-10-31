from __future__ import absolute_import

from flask import request, g, abort
from flask.views import MethodView

from huskar_api import settings
from huskar_api.models.auth import Application, Team, Authority
from huskar_api.models.manifest import application_manifest
from huskar_api.models.exceptions import NameOccupiedError
from huskar_api.service.admin.application_auth import (
    check_application_auth, check_application)
from huskar_api.service.organization.exc import ApplicationExistedError
from huskar_api.service.admin.exc import NoAuthError
from huskar_api.extras.auth import AppInfo
from .schema import application_schema, validate_email, validate_fields
from .utils import (
    login_required, api_response, minimal_mode_incompatible, audit_log,
    with_etag, with_cache_control)


class TeamView(MethodView):
    @login_required
    def get(self, team_name=None):
        """Gets the team list or application list.

        While ``team_name`` is specified, the application list in specified
        team will be responded. Otherwise, the team list will be responded.

        The response of team list looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": {
                "teams": [{"name": "team-1", "desc": "team-1"},
                          {"name": "team-2"}, "desc": "team-1"]
              }
            }

        And the response of application list looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": {
                "applications": ["base.foo", "base.bar"]
              }
            }

        :param team_name: The name of specified team.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The team with specified name is not found.
        :status 200: The team list or application list is responded.
        """
        if team_name:
            if g.auth.is_minimal_mode:
                if team_name != Team.DEFAULT_NAME:
                    abort(404, 'team "%s" does not exist' % team_name)
                applications = application_manifest.as_list()
            else:
                team = self._get_team_or_404(team_name)
                applications = Application.get_multi_by_team(team.id)
                applications = [x.application_name for x in applications]
            data = {'applications': applications}
        else:
            if g.auth.is_minimal_mode:
                data = {'teams': [{'name': Team.DEFAULT_NAME,
                                   'desc': Team.DEFAULT_NAME}]}
            else:
                teams = Team.get_all()
                data = {'teams': [{'name': x.team_name, 'desc': x.team_desc}
                                  for x in teams]}
        return api_response(data)

    @login_required
    @minimal_mode_incompatible
    def post(self):
        """Creates a new team.

        :form team: The name of creating team.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The name is invalid.
        :status 200: The team with specified name exists.
        :status 201: The team with specified name is created successfully.
        """
        g.auth.require_admin('only admin can add team')
        team_name = request.form['team'].strip()
        team = Team.get_by_name(team_name)
        if team is not None:
            return api_response(), 200

        try:
            team = Team.create(team_name)
        except NameOccupiedError:
            abort(400, 'Team %s has been archived.' % team_name)
        audit_log.emit(audit_log.types.CREATE_TEAM, team=team)
        return api_response(), 201

    @login_required
    @minimal_mode_incompatible
    def delete(self, team_name):
        """Deletes a team.

        :form team: The name of deleting team.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The team with specified name is not found.
        :status 200: The team with specified name is deleted successfully.
        """
        g.auth.require_admin('only admin can delete team')
        team = self._get_team_or_404(team_name)
        with audit_log(audit_log.types.ARCHIVE_TEAM, team=team):
            team.archive()
        return api_response()

    def _get_team_or_404(self, team_name):
        team = Team.get_by_name(team_name)
        if team is None:
            abort(404, 'team "%s" does not exist' % team_name)
        return team


class ApplicationView(MethodView):
    @login_required
    def get(self, application_name):
        """Gets an application by its name.

        The response looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": {
                "item": {
                  "name": "foo.test",
                  "team_name": "foo",
                  "team_desc": "foo-bar",
                  "is_deprecated": false,
                  "is_blacklisted": false,
                  "route_stage": {
                    "altc1-channel-stable-1": "S"
                  }
                }
              }
            }

        There is nothing acts HTTP cache in this API like the
        :ref:`application_list` was done.

        :param application_name: The name of deleting application.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application does not exist yet.
        :status 200: The application was returned successfully.
        """
        if g.auth.is_minimal_mode:
            if not application_manifest.check_is_application(application_name):
                abort(404, 'application does not exist')
            result = application_schema.dump({
                'name': application_name,
                'team_name': Team.DEFAULT_NAME,
                'team_desc': Team.DEFAULT_NAME,
            })
        else:
            application = Application.get_by_name(application_name)
            if application is None:
                abort(404, 'application does not exist')
            result = application_schema.dump({
                'name': application.application_name,
                'team_name': application.team.team_name,
                'team_desc': application.team.team_desc,
            })
        return api_response({'item': result.data})

    @login_required
    @minimal_mode_incompatible
    def delete(self, application_name):
        """Deletes an existed application.

        .. todo:: Extract the 401/404 from 401.

        :param application_name: The name of deleting application.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The application does not exist yet.
        :status 401: The token are invalid or expired.
        :status 403: The token don't have required authority on current
                     application.
        :status 200: The application was deleted successfully.
        """
        application = check_application(application_name)
        require_team_admin_or_site_admin(application.team)

        with audit_log(audit_log.types.ARCHIVE_APPLICATION,
                       application=application, team=application.team):
            application.archive()

        return api_response()


class ApplicationListView(MethodView):
    @login_required
    @with_cache_control
    @with_etag
    def get(self):
        """Gets the application list.

        In mimimal mode, this API returns application list from the ZooKeeper
        instead of MySQL. Some deleted applications will appear again.

        A successful response looks like a list of entities which defined in
        :ref:`application_item`,

        .. todo:: Extract the 401/404 from 401.

        :query with_authority: Optional. Passing ``1`` will let this API return
                               applications which authorized by current user.
                               Default is ``0`` (disabled).
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 401: The token are invalid or expired.
        :status 200: The application list is in the response:
                     ``{"status": "SUCCESS",
                     "data": [
                     {"name": "base.foo", "team_name": "Base", ...}]}``
        """
        with_authority = request.args.get('with_authority', type=int)
        with_authority = bool(with_authority)

        if g.auth.is_minimal_mode:
            result = application_schema.dump([
                {'name': name, 'team_name': Team.DEFAULT_NAME,
                 'team_desc': Team.DEFAULT_NAME}
                for name in application_manifest.as_list()
            ], many=True)
        else:
            application_list = Application.get_all()
            if with_authority and not g.auth.is_admin:
                application_list = [
                    item for item in application_list if (
                        item.check_auth(Authority.READ, g.auth.id) or
                        item.check_auth(Authority.WRITE, g.auth.id)
                    )
                ]
            result = application_schema.dump([
                {'name': item.application_name,
                 'team_name': item.team.team_name,
                 'team_desc': item.team.team_desc}
                for item in application_list
            ], many=True)

        return api_response(result.data)

    @login_required
    @minimal_mode_incompatible
    def put(self):
        """creates a new application.

        This method is same as **POST** but calling it once or several times
        successively has the same effect.

        :form team: The name of team to place the new application.
        :form application: The name of new application.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The application name is invalid, the application existed
                     already, or the target team does not exist yet.
        :status 401: The token are invalid or expired.
        :status 403: The token don't have required authority on current
                     application.
        :status 200: The application was created successfully.

        """
        return self._create(ignore_existed=True)

    @login_required
    @minimal_mode_incompatible
    def post(self):
        """Creates a new application.

        Only :ref:`site admin <site_admin>` and :ref:`team admin <team_admin>`
        are permitted to create new applications.

        .. note:: The name of application (a.k.a appid) should be **unique
                  globally**, whatever which team the application stays in.
        .. todo:: Extract the 401/404 from 401.

        :form team: The name of team to place the new application.
        :form application: The name of new application.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The application name is invalid, the application existed
                     already, or the target team does not exist yet.
        :status 401: The token are invalid or expired.
        :status 403: The token don't have required authority on current
                     application.
        :status 200: The application was created successfully.
        """
        return self._create(ignore_existed=False)

    def _create(self, ignore_existed=False):
        team_name = request.values['team'].strip()
        application_name = request.values['application'].strip()
        validate_fields(application_schema,
                        {'name': application_name, 'team_name': team_name})

        team = Team.get_by_name(team_name)
        if team is None:
            abort(400, 'team "%s" does not exist' % team_name)

        require_team_admin_or_site_admin(team)

        application = Application.get_by_name(application_name)
        if application is not None:
            if ignore_existed:
                return api_response()
            raise ApplicationExistedError(
                'application: {} has existed, application is globally '
                'unique'.format(application))

        try:
            application = Application.create(application_name, team.id)
        except NameOccupiedError:
            abort(400, 'The application name {0} has been occupied.'.format(
                application_name))
        audit_log.emit(
            audit_log.types.CREATE_APPLICATION, application=application,
            team=team)
        return api_response()


def require_team_admin_or_site_admin(team):
    if g.auth.is_admin or team.check_is_admin(g.auth.id):
        return
    raise NoAuthError('No admin authority on {}'.format(team.team_name))


class ApplicationTokenView(MethodView):
    @login_required
    @minimal_mode_incompatible
    def post(self, application_name):
        """Obtains an application token.

        For using this API, you need to have an user token already, and the
        user need to have **write** authority on current application.

        .. note:: Only :ref:`user-token` are acceptable in this API.

        .. todo:: Extract the 401/404 from 401.
        .. todo:: Restrict the app token.

        :param application_name: The name of application (a.k.a appid).
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The application is malformed. If you believe you are not
                     doing anything wrong, please contact the developers of
                     Huskar API (See :ref:`contact`).
        :status 401: The token are invalid or expired.
        :status 403: The token don't have required authority on current
                     application.
        :status 404: The application does not exist. You need to create it in
                     the first.
        :status 200: You could find token from the response body:
                     ``{"status": "SUCCESS", "data": {"token": "..",
                     "expires_in": None}}``
        """
        check_application_auth(application_name, Authority.WRITE)
        if (g.auth.is_application and
                g.auth.username not in settings.AUTH_SPREAD_WHITELIST):
            raise NoAuthError('It is not permitted to exchange token')
        application = check_application(application_name)
        try:
            user = application.setup_default_auth()
        except NameOccupiedError:
            abort(400, 'malformed application: %s' % application_name)

        token = user.generate_token(settings.SECRET_KEY, None)
        return api_response(data={'token': token, 'expires_in': None})

    @login_required
    def get(self, application_name):
        return self.post(application_name)


class TeamApplicationTokenView(MethodView):
    @login_required
    @minimal_mode_incompatible
    def post(self, team_name, application_name):
        """Ensures the related resources exist and gets an application token.

        .. note:: This is an internal API. We guarantee nothing if you are not
                  developers of Huskar and you are using this API.

        :param team_name: The name of team.
        :param application_name: The name of application.
        :status 400: The application_name is malformed.
        :status 403: Permission denied.
        :status 200: The token is in the response.
        """
        if not g.auth.is_admin:
            abort(403, 'user "%s" has no admin permission' % g.auth.username)

        owner_email = request.form['owner_email'].strip()
        validate_email(owner_email)
        owner_name = owner_email.split('@')[0]

        try:
            appinfo = AppInfo.from_external(
                team_name=team_name.lower(),
                application_name=application_name,
                owner_name=owner_name,
                owner_email=owner_email,
            )
            _, user = appinfo.submit_to_import()
        except NameOccupiedError:
            abort(400, 'malformed application: %s' % application_name)

        token = user.generate_token(settings.SECRET_KEY, None)
        return api_response(data={'token': token, 'expires_in': None})
