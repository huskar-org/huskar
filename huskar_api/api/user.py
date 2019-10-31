from __future__ import absolute_import

import uuid

from flask import g, request, abort
from flask.views import MethodView
from werkzeug.security import gen_salt

from huskar_api.models.auth import User
from huskar_api.models.exceptions import NameOccupiedError
from huskar_api.extras.email import deliver_email, EmailTemplate
from huskar_api.service.admin.user import (
    change_email,
    request_to_reset_password,
    reset_password,
)
from .utils import (
    login_required, api_response, minimal_mode_incompatible, audit_log)
from .schema import user_schema, validate_fields


class UserView(MethodView):
    @login_required
    def get(self, username=None):
        """Gets the entity data of an user or a list of users.

        :param username: If specified then respond an user instead a list of
                         users.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The username is specified but there is nothing found.
        :status 200: An user or a list of users.
        """
        if username:
            user = self._get_user_or_404(username)
            return api_response(user_schema.dump(user).data)
        else:
            user_list = User.get_all_normal()
            return api_response(user_schema.dump(user_list, many=True).data)

    @login_required
    @minimal_mode_incompatible
    def post(self):
        """Creates a new user with your site admin authority.

        We will send you an email of random password if don't specify password
        explicitly.

        :form username: The username of new user.
        :form password: The optional password of new user.
        :form email: The email of new user.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 400: The username is used or the format is invalid.
        :status 200: The new user is created successfully.
        """
        g.auth.require_admin('only admin can add users')

        username = request.form['username'].strip()
        password = request.form.get('password', gen_salt(30))
        is_generated_password = 'password' not in request.form
        email = request.form['email'].strip()
        validate_fields(user_schema, {'username': username, 'email': email})

        user = User.get_by_name(username)
        if user:
            abort(400, u'{0} is used username'.format(username))

        try:
            user = User.create_normal(username, password, email,
                                      is_active=True)
        except NameOccupiedError:
            abort(400, u'User %s has been archived' % username)
        audit_log.emit(audit_log.types.CREATE_USER, user=user)

        if is_generated_password:
            deliver_email(EmailTemplate.SIGNUP, user.email, {
                'username': user.username,
                'password': password,
            })

        return api_response()

    @login_required
    @minimal_mode_incompatible
    def delete(self, username):
        """Deletes an user permanently with your site admin authority.

        :param username: The username of deleting user.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The user is not found.
        :status 200: The user is deleted successfully.
        """
        g.auth.require_admin('only admin can delete users')

        user = self._get_user_or_404(username)
        with audit_log(audit_log.types.ARCHIVE_USER, user=user):
            user.archive()
        return api_response()

    @login_required
    @minimal_mode_incompatible
    def put(self, username):
        """Changes the password or email of specified user.

        :param username: The username of specified user.
        :form old_password: The old passwor of specified user.
        :form new_password: Optional. If you want to modify your password,
                            specify it.
        :form email: Optional. If you want to modify your email, specify it.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 403: The old password is incorrect or this modification is
                     not permitted.
        :status 404: The user is not found.
        :status 200: The user is modified successfully.
        """
        old_password = request.form['old_password']
        new_password = request.form.get('new_password')
        new_email = request.form.get('email')

        if g.auth.username != username:
            abort(403, '%s is not permitted to modify %s.' % (
                g.auth.username, username
            ))
        validate_fields(user_schema, {'email': new_email},
                        optional_fields=['email'], partial=True)

        user = self._get_user_or_404(username)
        if not user.check_password(old_password):
            abort(403, 'password is not correct')

        if new_password:
            with audit_log(audit_log.types.CHANGE_USER_PASSWORD, user=user):
                user.change_password(new_password)
        if new_email:
            change_email(user, new_email)

        return api_response()

    def _get_user_or_404(self, username):
        user = User.get_by_name(username)
        if user is None or user.is_application:
            abort(404, u'user {} not found'.format(username))
        return user


class PasswordResetView(MethodView):
    @minimal_mode_incompatible
    def post(self, username):
        """Resets password of specified user.

        If you request this API without the optional ``token`` argument, the
        specified user will receive a validation email which contains a token.
        The token will expire in ten minutes.

        Once you request this API again with the valid token, the password of
        specified user will be reset into the new one you provided.

        :param username: The username of specified user.
        :form token: Optional. The token contained in the validation email.
        :form password: Optional. The new password you request to set.
        :status 200: The request is successful. The ``{"email": "..."}`` will
                     be responded while requesting to reset password. If the
                     value is ``null`` then you can not reset password of this
                     user without help of administrators.
        """
        token = request.form.get('token', type=uuid.UUID)
        if token:
            new_password = request.form['password']
            user = reset_password(username, token, new_password)
            audit_log.emit(audit_log.types.CHANGE_USER_PASSWORD, user=user)
            return api_response()
        else:
            user, _ = request_to_reset_password(username)
            audit_log.emit(audit_log.types.FORGOT_USER_PASSWORD, user=user)
            return api_response({'email': user.email})
