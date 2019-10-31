from __future__ import absolute_import

import logging
import copy

from flask import json
from sqlalchemy import Column, Integer, BigInteger, Unicode, UnicodeText
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.utils import cached_property

from huskar_api import settings
from huskar_api.extras.monitor import monitor_client
from huskar_api.extras.raven import capture_exception
from huskar_api.models import (
    DeclarativeBase, TimestampMixin, CacheMixin, DBSession)
from huskar_api.models.auth import (
    User, Team, Application, Authority)
from huskar_api.models.signals import new_action_detected
from huskar_api.models.utils import take_slice
from huskar_api.models.exceptions import (
    AuditLogTooLongError, AuditLogLostError)
from .action import action_types, action_creator
from .rollback import action_rollback
from .const import (
    TYPE_SITE, TYPE_TEAM, TYPE_APPLICATION, TYPE_CONFIG,
    TYPE_SWITCH, TYPE_SERVICE, NORMAL_USER, APPLICATION_USER,
    SEVERITY_NORMAL, SEVERITY_DANGEROUS
)
from .index import (
    AuditIndex, AuditIndexInstance, create_index, flush_index_cache)

logger = logging.getLogger(__name__)


class AuditLog(TimestampMixin, CacheMixin, DeclarativeBase):
    """The audit log entry."""

    __tablename__ = 'audit_log'

    # Expose constants
    TYPE_SITE = TYPE_SITE
    TYPE_TEAM = TYPE_TEAM
    TYPE_APPLICATION = TYPE_APPLICATION
    TYPE_CONFIG = TYPE_CONFIG
    TYPE_SWITCH = TYPE_SWITCH
    TYPE_SERVICE = TYPE_SERVICE

    # do not create the audit log if it's larger than the limit length.
    # https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
    MAX_AUDIT_LENGTH = 65530

    id = Column(BigInteger, primary_key=True)
    user_id = Column(Integer, nullable=False, index=True)
    remote_addr = Column(Unicode(20, collation='utf8mb4_bin'), nullable=False)
    action_type = Column(Integer, nullable=False)
    action_data = Column(UnicodeText(2048, collation='utf8mb4_bin'),
                         nullable=False)
    rollback_id = Column(BigInteger, index=True)

    @classmethod
    def create(cls, user_id, remote_addr, action, rollback_to=None):
        """Creates a new audit log.

        :param int user_id: The id of operator.
        :param str remote_addr: The IP address of operator.
        :param tuple action: Use :meth:`action_creator.make_action` to create
                             this tuple.
        :param int rollback_to: Optional. Default is :obj:`None`. If this is
                                a rollback operation, pass id of the audit log
                                want to rollback.
        :returns: The created instance.
        """
        if rollback_to is not None and cls.get(rollback_to) is None:
            raise ValueError('rollback_to is not a valid id')

        action_type, action_data, action_indices = action
        trace_all_application_events(action_type, action_data)
        action_data = json.dumps(
            action_data, sort_keys=True, ensure_ascii=False)
        if len(action_data) >= cls.MAX_AUDIT_LENGTH:
            _publish_new_action(user_id, remote_addr, action)
            raise AuditLogTooLongError('The audit log is too long.')

        instance = cls(
            user_id=user_id, remote_addr=remote_addr, action_type=action_type,
            action_data=action_data, rollback_id=rollback_to)
        try:
            with DBSession().close_on_exit(False) as db:
                db.add(instance)
                db.flush()
                for args in action_indices:
                    create_index(db, instance.id, instance.created_at, args)
        except SQLAlchemyError:
            _publish_new_action(user_id, remote_addr, action)
            raise AuditLogLostError()
        date = instance.created_at.date()
        for index_args in action_indices:
            flush_index_cache(date, index_args)
        _publish_new_action(user_id, remote_addr, action)
        return instance

    @classmethod
    def get_multi_and_prefetch(cls, ids):
        """Gets multiple instances and prefetches their nested instances.

        :param list ids: The list of id.
        :returns: The list of instances.
        """
        instances = cls.mget(ids)

        uids = [x.user_id for x in instances]
        umap = User.mget(uids, as_dict=True)
        rids = [x.rollback_id for x in instances if x.rollback_id is not None]
        rmap = cls.mget(rids, as_dict=True)

        for instance in instances:
            if instance.user_id in umap:
                instance.user = umap[instance.user_id]
            if instance.rollback_id in rmap:
                instance.rollback_to = rmap[instance.rollback_id]

        return instances

    @classmethod
    def get_multi_by_index(cls, target_type, target_id):
        """Gets multiple instances from the audit indices.

        :param int target_type: The constant like :const:`AuditLog.TYPE_TEAM`.
        :param int target_id: The id of target model.
        :returns: The list of instances.
        """
        ids = AuditIndex.get_audit_ids(target_type, target_id)
        return take_slice(cls.get_multi_and_prefetch, ids)

    @classmethod
    def get_multi_by_index_with_date(cls, target_type, target_id, date):
        ids = AuditIndex.get_audit_ids_by_date(target_type, target_id, date)
        return take_slice(
            cls.get_multi_and_prefetch, sorted(ids, reverse=True))

    @classmethod
    def get_multi_by_instance_index(
            cls, instance_type, application_id, cluster_name,
            instance_key):
        """Gets multiple instances by instance information.

        :param application_id: The name of application instance.
        :param cluster_name: The name of cluster, it's optional.
        :param instance_key: The name of instance key.
        :param instance_type: The type of the instance.
        :returns: The list of instances.
        """
        ids = AuditIndexInstance.get_audit_ids(
            instance_type, application_id, cluster_name, instance_key)
        return take_slice(cls.get_multi_and_prefetch, ids)

    @classmethod
    def can_view_sensitive_data(cls, user_id, target_type, target_id):
        user = User.get(user_id)
        if user and user.is_admin:
            return True

        if target_type == cls.TYPE_TEAM:
            team = Team.get(target_id)
            return team and team.check_is_admin(user_id)

        if target_type in (cls.TYPE_APPLICATION, cls.TYPE_SERVICE,
                           cls.TYPE_CONFIG, cls.TYPE_SWITCH):
            application = Application.get(target_id)
            return application and application.check_auth(
                Authority.READ, user_id)
        return False

    def desensitize(self):
        insensitive_data = remove_sensitive_data(json.loads(self.action_data))
        action_data = json.dumps(
            insensitive_data, sort_keys=True, ensure_ascii=False)
        return dict(
            id=self.id,
            user=self.user,
            remote_addr=self.remote_addr,
            action_name=self.action_name,
            action_data=action_data,
            created_at=self.created_at,
            rollback_to=self.rollback_to)

    @cached_property
    def user(self):
        """The user model of operator."""
        return User.get(self.user_id)

    @cached_property
    def action_name(self):
        """The name of action type."""
        return action_types[self.action_type]

    @cached_property
    def rollback_to(self):
        """The action log which rollbacked by this action."""
        if self.rollback_id is None:
            return
        return self.get(self.rollback_id)

    @cached_property
    def can_rollback(self):
        return action_rollback.can_rollback(self.action_type)

    def rollback(self, user_id, remote_addr):
        action_data = json.loads(self.action_data)
        action_type, components = action_rollback.rollback(
            self.action_type, action_data)
        action = action_creator.make_action(action_type, **components)
        return self.create(user_id, remote_addr, action, rollback_to=self.id)


def remove_sensitive_data(action_data):
    insensitive_data = copy.deepcopy(action_data)
    insensitive_data.pop('data', None)
    insensitive_data.pop('value', None)
    insensitive_data.pop('nested', None)
    return insensitive_data


def _publish_new_action(user_id, remote_addr, action):
    if remote_addr == settings.LOCAL_REMOTE_ADDR:
        return

    user = User.get(user_id)
    user_type = NORMAL_USER
    username = None
    if user:
        username = user.username
        if (user.is_application or
                username in settings.APPLICATION_USE_USER_TOKEN_USER_LIST):
            user_type = APPLICATION_USER
    is_subscriable = any(
        item[0] == TYPE_APPLICATION
        for item in action.action_indices
    )

    severity = SEVERITY_DANGEROUS
    action_name = action_types[action.action_type]
    if action_name in settings.DANGEROUS_ACTION_NAMES_EXCLUDE_LIST:
        severity = SEVERITY_NORMAL

    insensitive_data = remove_sensitive_data(action.action_data)
    try:
        new_action_detected.send(
            AuditLog,
            action_type=action.action_type,
            username=username,
            user_type=user_type,
            action_data=insensitive_data,
            is_subscriable=is_subscriable,
            severity=severity
        )
    except Exception:
        logger.exception('Unexpected error of publish webhook event')
        capture_exception(data=None)


def trace_all_application_events(action_type, action_data):
    if not isinstance(action_data, dict):
        return

    action_name = action_types[action_type]
    application_names = (action_data.get('application_names') or
                         [action_data.get('application_name')])

    for application_name in application_names:
        if not application_name:
            continue
        monitor_client.increment('audit.application_event', tags={
            'action_name': action_name,
            'application_name': application_name,
        })
