from __future__ import absolute_import

from sqlalchemy import Column, Integer, Unicode, Index
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.exc import IntegrityError

from huskar_api.models import (
    DeclarativeBase, TimestampMixin, CacheMixin, DBSession,
    cache_on_arguments)


class Webhook(TimestampMixin, CacheMixin, DeclarativeBase):
    __tablename__ = 'webhook'

    TYPE_NORMAL = 0
    TYPE_UNIVERSAL = 1
    HOOK_TYPES = (TYPE_NORMAL, TYPE_UNIVERSAL)

    id = Column(Integer, primary_key=True)
    url = Column(Unicode(2000, collation='utf8mb4_bin'), nullable=False)
    hook_type = Column(TINYINT, nullable=False, default=TYPE_NORMAL)

    @classmethod
    def create(cls, url, hook_type=TYPE_NORMAL):
        with DBSession().close_on_exit(False) as db:
            instance = cls(url=url, hook_type=hook_type)
            db.add(instance)
        cls.flush([instance.id])
        cls.get_all_ids.flush()
        cls.get_ids_by_type.flush(hook_type)
        return instance

    def update_url(self, url):
        with DBSession().close_on_exit(False):
            self.url = url
        self.flush([self.id])

    def delete(self):
        with DBSession().close_on_exit(False) as db:
            db.delete(self)
        self.flush([self.id])
        self.get_all_ids.flush()
        self.get_ids_by_type.flush(self.hook_type)

    @classmethod
    def get_all(cls):
        ids = cls.get_all_ids()
        return cls.mget(ids)

    @classmethod
    @cache_on_arguments(10 * 60)
    def get_all_ids(cls):
        rs = DBSession().query(cls.id).all()
        return sorted(r[0] for r in rs)

    @classmethod
    def get_all_universal(cls):
        ids = cls.get_ids_by_type(cls.TYPE_UNIVERSAL)
        return cls.mget(ids)

    @classmethod
    @cache_on_arguments(10 * 60)
    def get_ids_by_type(cls, hook_type):
        rs = DBSession().query(cls.id).filter_by(hook_type=hook_type).all()
        return sorted([r[0] for r in rs])

    @property
    def is_normal(self):
        return self.hook_type == self.TYPE_NORMAL

    def subscribe(self, application_id, action_type):
        if self.hook_type == self.TYPE_UNIVERSAL:
            return

        sub = self.get_subscription(application_id, action_type)
        if sub is None:
            try:
                sub = WebhookSubscription.create(
                    application_id, self.id, action_type)
            except IntegrityError:
                return None
        return sub

    def unsubscribe(self, application_id, action_type):
        sub = self.get_subscription(application_id, action_type)
        if sub is not None:
            sub.delete()

    def batch_unsubscribe(self, application_id=None):
        subs = WebhookSubscription.search_by(application_id, self.id)
        for sub in subs:
            sub.delete()

    def get_subscription(self, application_id, action_type):
        return WebhookSubscription.find(application_id, self.id, action_type)

    def get_multi_subscriptions(self, application_id):
        return WebhookSubscription.search_by(application_id, self.id)

    @classmethod
    def search_subscriptions(cls, application_id=None, webhook_id=None,
                             action_type=None):
        return WebhookSubscription.search_by(
            application_id=application_id,
            webhook_id=webhook_id,
            action_type=action_type)


class WebhookSubscription(TimestampMixin, CacheMixin, DeclarativeBase):
    __tablename__ = 'webhook_subscription'
    id = Column(Integer, primary_key=True)
    action_type = Column(Integer, nullable=False)
    application_id = Column(Integer, nullable=False)
    webhook_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index('ix_webhook_subscription_app_type', 'application_id',
              'action_type', unique=False),
        Index('ix_webhook_subscription_hook_type', 'webhook_id',
              'action_type', unique=False),
        UniqueConstraint('application_id', 'webhook_id', 'action_type',
                         name='ux_webhook_subscription'),
        DeclarativeBase.__table_args__,
    )

    @classmethod
    def create(cls, application_id, webhook_id, action_type):
        with DBSession().close_on_exit(False) as db:
            instance = cls(webhook_id=webhook_id,
                           application_id=application_id,
                           action_type=action_type)
            db.add(instance)
        cls.flush([cls.id])
        cls.get_id.flush(application_id, webhook_id, action_type)
        cls.get_ids.flush(application_id, webhook_id, action_type)
        cls.get_ids.flush(application_id, webhook_id, None)
        cls.get_ids.flush(application_id, None, None)
        return instance

    @classmethod
    def find(cls, application_id, webhook_id, action_type):
        subscription_id = cls.get_id(application_id, webhook_id, action_type)
        if subscription_id is not None:
            return cls.get(subscription_id)

    @classmethod
    def search_by(cls, application_id=None, webhook_id=None,
                  action_type=None):
        ids = cls.get_ids(application_id, webhook_id, action_type)
        return cls.mget(ids)

    @classmethod
    @cache_on_arguments(10 * 60)
    def get_id(cls, application_id, webhook_id, action_type):
        ids = cls.get_ids(application_id, webhook_id, action_type)
        return ids[0] if ids else None

    @classmethod
    @cache_on_arguments(10 * 60)
    def get_ids(cls, application_id, webhook_id, action_type):
        conds = {name: value for name, value in [
            ('application_id', application_id),
            ('webhook_id', webhook_id),
            ('action_type', action_type),
        ] if value is not None}

        rs = DBSession().query(cls.id).filter_by(**conds).all()
        return [r[0] for r in rs]

    def delete(self):
        with DBSession().close_on_exit(False) as db:
            db.delete(self)
        self.flush([self.id])
        self.get_id.flush(
            self.application_id, self.webhook_id, self.action_type)
        self.get_ids.flush(
            self.application_id, self.webhook_id, self.action_type)
        self.get_ids.flush(
            self.application_id, self.webhook_id, None)
        self.get_ids.flush(
            self.application_id, None, None)

    @property
    def webhook(self):
        return Webhook.get(self.webhook_id)
