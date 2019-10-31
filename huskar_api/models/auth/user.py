from __future__ import absolute_import

import hashlib
import logging

import itsdangerous
from sqlalchemy import Column, Integer, Unicode, DateTime, Boolean
from sqlalchemy.exc import IntegrityError
from werkzeug.security import safe_str_cmp

from huskar_api import settings
from huskar_api.extras.monitor import monitor_client
from huskar_api.models.db import UpsertMixin
from huskar_api.models import (
    DeclarativeBase, TimestampMixin, CacheMixin, DBSession, cache_on_arguments)
from huskar_api.models.exceptions import NameOccupiedError
from huskar_api.models.signals import user_grant_admin, user_dismiss_admin

logger = logging.getLogger(__name__)


class User(TimestampMixin, CacheMixin, UpsertMixin, DeclarativeBase):
    """The user of Huskar API."""

    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    username = Column(Unicode(128, collation='utf8mb4_bin'),
                      nullable=False, unique=True)
    password = Column(Unicode(128, collation='utf8mb4_bin'), nullable=False)
    email = Column(Unicode(128, collation='utf8mb4_bin'), unique=True)
    last_login = Column(DateTime)
    is_active = Column(Boolean, nullable=False, default=True)
    huskar_admin = Column(Boolean, nullable=False, default=False)
    is_app = Column(Boolean, nullable=False, default=False)

    @property
    def is_admin(self):
        """``True`` if the user is a site administrator."""
        return self.huskar_admin

    @property
    def is_application(self):
        """``True`` if the user is an application identity."""
        return self.is_app

    @classmethod
    def get_by_token(cls, secret_key, token, raises=False):
        """Gets an user instance by its token.

        This method is a shortcut of :meth:`User.parse_username_in_token`
        and :meth:`User.get_by_name`.

        :param str secret_key: The secret key for HMAC signature of JWT.
        :param str token: The token value.
        :param bool raises: ``True`` if you want to raise exceptions while
                            token is invalid or expired.
        :returns: An :class:`User` instance or ``None``.
        """
        username = cls.parse_username_in_token(secret_key, token, raises)
        if username:
            return cls.get_by_name(username)

    @classmethod
    def parse_username_in_token(cls, secret_key, token, raises=False):
        """Parses the token and returns the username inside it.

        :param str secret_key: The secret key for HMAC signature of JWT.
        :param str token: The token value.
        :param bool raises: ``True`` if you want to raise exceptions while
                            token is invalid or expired.
        :returns: The username string or ``None``.
        """
        try:
            res = _load_token(secret_key, token, raises)
            if res is None:
                res = _load_fallback_token(token)
            return res
        except Exception:  # pragma: no cover
            # TODO: Deprecate this after lockdown
            res = _load_fallback_token(token)
            if res is None:
                raise
            return res

    @classmethod
    def get_by_name(cls, name):
        """Gets an user by its username.

        :param str name: The input username.
        :returns: An :class:`User` instance or ``None``.
        """
        user_id = cls.get_id_by_name(name)
        if user_id is not None:
            return cls.get(user_id)

    @classmethod
    def get_by_email(cls, email):
        """Gets an user by its email.

        :param str name: The input username.
        :returns: An :class:`User` instance or ``None``.
        """
        user_id = cls.get_id_by_email(email)
        if user_id is not None:
            return cls.get(user_id)

    @classmethod
    def get_all_normal(cls):
        """Gets all normal users.

        The application users are excluded.

        :returns: A list of :class:`User` instances.
        """
        ids = cls.get_ids_of_normal()
        return cls.mget(ids)

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_id_by_name(cls, name):
        return DBSession().query(cls.id).filter_by(
            username=name,
            is_active=True
        ).scalar()

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_id_by_email(cls, email):
        return DBSession().query(cls.id).filter_by(
            email=email,
            is_active=True
        ).scalar()

    @classmethod
    @cache_on_arguments(5 * 60)
    def get_ids_of_normal(cls):
        rs = DBSession().query(cls.id) \
                        .filter_by(is_app=False, is_active=True) \
                        .order_by(cls.id.asc()).all()
        return [r[0] for r in rs]

    @classmethod
    def create_normal(cls, username, password, email=None, is_active=False):
        """Creates a normal user.

        :param username: The unique name of user. It can not be the same to
                         existed users and applications.
        :param password: The plain text password of user.
        :param email: The optional email address of user.
        :param is_active: Only ``True`` is available for now.
        :returns User: The new created user.
        """
        try:
            with DBSession().close_on_exit(False) as db:
                instance = cls(
                    username=username,
                    password=_hash_password(password),
                    email=email,
                    is_active=int(bool(is_active)),
                    huskar_admin=False,
                    is_app=False)
                db.add(instance)
        except IntegrityError:
            raise NameOccupiedError

        cls.flush([instance.id])
        cls.get_id_by_name.flush(username)
        cls.get_ids_of_normal.flush()
        cls.get_id_by_email.flush(email)
        return instance

    @classmethod
    def create_application(cls, application_name):
        stmt = cls.upsert().values(
            username=application_name,
            password=_hash_password(application_name),
            email=None,
            is_active=True,
            is_app=True)

        with DBSession().close_on_exit(False) as db:
            rs = db.execute(stmt)

        instance = cls.get(rs.lastrowid)
        DBSession().refresh(instance)

        cls.get_id_by_name.flush(application_name)
        cls.get_ids_of_normal.flush()
        cls.flush([instance.id])

        return instance

    def _set_active_status(self, is_active):
        """Set value of `is_active` column to
        disable or active this user.
        """
        with DBSession().close_on_exit(False):
            self.is_active = is_active

        self.__class__.get_id_by_name.flush(self.username)
        self.__class__.get_ids_of_normal.flush()
        self.__class__.flush([self.id])

    def archive(self):
        self._set_active_status(is_active=False)

    def unarchive(self):
        self._set_active_status(is_active=True)

    def check_password(self, input_):
        _not_implemented_for_application(self)
        return _check_password(input_, self.password)

    def change_password(self, input_):
        _not_implemented_for_application(self)
        with DBSession().close_on_exit(False):
            self.password = _hash_password(input_)
        self.flush([self.id])

    def grant_admin(self):
        _not_implemented_for_application(self)
        with DBSession().close_on_exit(False):
            self.huskar_admin = True
        self.flush([self.id])
        user_grant_admin.send('user', user_id=self.id)

    def dismiss_admin(self):
        _not_implemented_for_application(self)
        with DBSession().close_on_exit(False):
            self.huskar_admin = False
        self.flush([self.id])
        user_dismiss_admin.send('user', user_id=self.id)

    def generate_token(self, secret_key, expires_in=None):
        return _dump_token(secret_key, self.username, expires_in)


def _hash_password(plain_password):
    plain_password = plain_password.encode('utf-8')
    return hashlib.sha224(plain_password).hexdigest().decode('ascii')


def _check_password(input_, stored):
    return safe_str_cmp(_hash_password(input_), stored)


def _dump_token(secret_key, username, expires_in=None):
    if expires_in is None:
        # simplejson is required for (un)serializing infinite value.
        # https://simplejson.readthedocs.org/en/latest/#infinite-and-nan-number-values
        expires_in = float('inf')
    s = itsdangerous.TimedJSONWebSignatureSerializer(
        secret_key, expires_in=expires_in)
    return s.dumps({'username': username})


def _load_token(secret_key, token, raises):
    s = itsdangerous.TimedJSONWebSignatureSerializer(secret_key)
    try:
        payload = s.loads(token)
    except (itsdangerous.BadSignature, itsdangerous.SignatureExpired):
        if raises:
            raise
        return
    return payload['username']


def _load_fallback_token(token):
    res = None
    for secret_key in settings.FALLBACK_SECRET_KEYS:
        res = _load_token(secret_key, token, False)
        if res:
            monitor_client.increment('old_token', tags=dict(res=res))
            logger.info('fallback token %s is detected', res)
            break
    return res


def _not_implemented_for_application(user):
    if user.is_application:
        raise NotImplementedError()
