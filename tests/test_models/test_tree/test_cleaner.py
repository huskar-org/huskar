from __future__ import absolute_import

import datetime
import time

from freezegun import freeze_time
import gevent

from huskar_api import settings
from huskar_api.switch import (
    SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK,
    SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN)
from huskar_api.models import redis_client
from huskar_api.models.tree import TreeHolderCleaner, TreeHub

REDIS_KEY = 'huskar_api.tree_holder_cleaner'


def test_track_success(mocker, mock_switches):
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: True,
    })
    cleaner = TreeHolderCleaner(TreeHub(mocker.Mock()))
    application_name = 'foo.test'
    type_name = 'config'
    n = time.time()

    cleaner.track(application_name, type_name)
    name = '{}:{}'.format(application_name, type_name)
    items = redis_client.zrange(
        REDIS_KEY, 0, -1, withscores=True)
    assert len(items) == 1
    assert items[0][0] == name
    assert n < items[0][1] < time.time()


def test_track_failed_or_skipped(mocker, mock_switches):
    cleaner = TreeHolderCleaner(TreeHub(mocker.Mock()))
    application_name = 'foo.test'
    type_name = 'config'

    # switch off, skip
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: False,
    })

    cleaner.track(application_name, type_name)
    items = redis_client.zrange(
        REDIS_KEY, 0, -1, withscores=True)
    assert not items

    # exception, ignore error
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: True,
    })
    mocker.patch.object(redis_client, 'zadd', side_effect=Exception())
    logger = mocker.patch('huskar_api.models.tree.cleaner.logger')
    cleaner.track(application_name, type_name)
    items = redis_client.zrange(
        REDIS_KEY, 0, -1, withscores=True)
    assert not items

    assert logger.warning.called
    call_args = logger.warning.call_args_list[0][0]
    assert call_args[0] == 'tree holder cleaner track item failed: %s'


def test_clean_success(mocker, mock_switches):
    tree_hub = TreeHub(mocker.Mock())
    tree_holder = mocker.MagicMock()
    cleaner = TreeHolderCleaner(tree_hub)
    cleaner._old_offset = 0
    application_name = 'foo.test'
    type_name = 'config'
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: True,
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN: True,
    })
    tree_hub.tree_map[(application_name, type_name)] = tree_holder
    tree_hub.tree_map[(application_name, 'switch')] = mocker.MagicMock()
    tree_hub.tree_map[(application_name, 'service')] = mocker.MagicMock()

    cleaner.track(application_name, type_name)
    cleaner.track(application_name, 'service')
    with freeze_time(datetime.datetime.now() + datetime.timedelta(days=1)):
        cleaner.track(application_name, 'switch')
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 3
    assert (application_name, type_name) in tree_hub.tree_map
    assert (application_name, 'switch') in tree_hub.tree_map
    assert (application_name, 'service') in tree_hub.tree_map

    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_CONDITION', 'True')
    cleaner.clean()
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 1
    assert (application_name, type_name) not in tree_hub.tree_map
    assert (application_name, 'switch') in tree_hub.tree_map
    assert (application_name, 'service') not in tree_hub.tree_map
    assert tree_holder.close.called


def test_clean_failed_or_skipped(mocker, mock_switches):
    tree_hub = TreeHub(mocker.Mock())
    tree_holder = mocker.MagicMock()
    cleaner = TreeHolderCleaner(tree_hub)
    cleaner._old_offset = 0
    application_name = 'foo.test'
    type_name = 'config'
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: False,
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN: True,
    })
    tree_hub.tree_map[(application_name, type_name)] = tree_holder
    tree_hub.tree_map[(application_name, 'switch')] = mocker.MagicMock()
    tree_hub.tree_map[(application_name, 'service')] = mocker.MagicMock()

    cleaner.track(application_name, type_name)
    cleaner.track(application_name, 'service')
    cleaner.track(application_name, 'switch')
    # track switch off, skip
    cleaner.clean()
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert not items

    # clean switch off, skip
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: True,
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN: False,
    })
    cleaner.track(application_name, type_name)
    cleaner.track(application_name, 'service')
    with freeze_time(datetime.datetime.now() + datetime.timedelta(days=1)):
        cleaner.track(application_name, 'switch')
    cleaner.clean()
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 3
    assert len(tree_hub.tree_map) == 3

    # condition empty, skip
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: True,
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN: True,
    })
    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_CONDITION', '')
    cleaner.clean()
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 3
    assert len(tree_hub.tree_map) == 3

    # condition false, skip
    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_CONDITION', 'cpu < 0')
    cleaner.clean()
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 3
    assert len(tree_hub.tree_map) == 3

    # condition invalid, skip
    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_CONDITION', 'a')
    logger = mocker.patch('huskar_api.models.tree.cleaner.logger')
    cleaner.clean()
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 3
    assert len(tree_hub.tree_map) == 3
    assert logger.error.called
    call_args = logger.error.call_args_list[0][0]
    assert call_args[0] == 'invalid tree holder cleaner condition: %r %s'
    logger.reset_mock()

    # get redis data failed, skip
    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_CONDITION', 'True')
    logger = mocker.patch('huskar_api.models.tree.cleaner.logger')
    orig_zrangebyscore = redis_client.zrangebyscore
    mocker.patch.object(redis_client, 'zrangebyscore', side_effect=Exception())
    cleaner.clean()
    mocker.patch.object(redis_client, 'zrangebyscore', orig_zrangebyscore)
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 3
    assert len(tree_hub.tree_map) == 3
    assert logger.warning.called
    call_args = logger.warning.call_args_list[0][0]
    assert call_args[0] == 'get tree holder cleaner data failed: %s'
    logger.reset_mock()

    tree_holder.close.reset_mock()
    cleaner.clean()
    tree_holder.close.reset_mock()
    cleaner.track(application_name, type_name)
    cleaner.clean()
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 1
    assert len(tree_hub.tree_map) == 1
    assert (application_name, type_name) not in tree_hub.tree_map
    assert (application_name, 'service') not in tree_hub.tree_map
    assert not tree_holder.close.called


def test_clean_thread(mocker, mock_switches):
    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_CONDITION', 'True')
    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_OLD_OFFSET', 0)
    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_PERIOD', 1)
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: True,
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN: True,
    })
    application_name = 'foo.test'
    type_name = 'config'
    tree_holder = mocker.MagicMock()
    tree_hub = TreeHub(mocker.Mock())
    tree_hub.tree_map[(application_name, type_name)] = tree_holder
    tree_hub.tree_map[(application_name, 'switch')] = mocker.MagicMock()
    tree_hub.tree_map[(application_name, 'service')] = mocker.MagicMock()
    cleaner = TreeHolderCleaner(tree_hub)
    cleaner.spawn_cleaning_thread()
    gevent.sleep(0.1)
    cleaner.track(application_name, type_name)
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 1
    assert (application_name, type_name) in tree_hub.tree_map
    gevent.sleep(2)
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 0
    assert (application_name, type_name) not in tree_hub.tree_map
    cleaner._stopped.set()
    cleaner.spawn_cleaning_thread()
    gevent.sleep(0.1)


def test_update_tree_holder_cleaner_condition():
    data = 'cpu < 80 and memory > 80'
    try:
        assert settings.TREE_HOLDER_CLEANER_CONDITION == ''
        settings.update_tree_holder_cleaner_condition(data)
        assert settings.TREE_HOLDER_CLEANER_CONDITION == data
    finally:
        settings.TREE_HOLDER_CLEANER_CONDITION = ''


def test_clean_old_redis_data(mocker, mock_switches):
    mocker.patch.object(
        settings, 'TREE_HOLDER_CLEANER_CONDITION', 'memory > 0')
    mocker.patch.object(settings, 'TREE_HOLDER_CLEANER_OLD_OFFSET', 2)
    tree_hub = TreeHub(mocker.Mock())
    cleaner = TreeHolderCleaner(tree_hub)
    application_name = 'foo.test'
    mock_switches({
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_TRACK: True,
        SWITCH_ENABLE_TREE_HOLDER_CLEANER_CLEAN: True,
    })
    now = datetime.datetime.now()
    cleaner.track(application_name, 'config')
    with freeze_time(now + datetime.timedelta(days=3)):
        cleaner.track(application_name, 'switch')
    with freeze_time(now + datetime.timedelta(days=-3)):
        cleaner.track(application_name, 'service')
    with freeze_time(now + datetime.timedelta(days=-4)):
        cleaner.track(application_name, 'foo')
    with freeze_time(now + datetime.timedelta(days=-8)):
        cleaner.track(application_name, '233')
    with freeze_time(now + datetime.timedelta(days=-10)):
        cleaner.track(application_name, '666')

    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 6
    cleaner.clean()
    items = redis_client.zrange(REDIS_KEY, 0, -1)
    assert len(items) == 4
    assert set(items) == set([
        '{}:config'.format(application_name),
        '{}:switch'.format(application_name),
        '{}:service'.format(application_name),
        '{}:foo'.format(application_name),
    ])

    # ignore error
    mocker.patch.object(
        redis_client, 'zremrangebyscore', side_effect=Exception())
    logger = mocker.patch('huskar_api.models.tree.cleaner.logger')
    cleaner.clean()
    assert logger.warning.called
    call_args = logger.warning.call_args_list[0][0]
    assert call_args[0] == 'clean tree holder cleaner old data failed: %s'
