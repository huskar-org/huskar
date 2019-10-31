from __future__ import absolute_import

import sys
from pytest import raises, mark

from huskar_api.models.utils import (
    take_slice, check_znode_path, normalize_cluster_name, dedupleft,
    merge_instance_list, retry)


def test_take_slice(mocker):
    mocker.spy(sys.modules[__name__], '_square')
    s = take_slice(_square, range(1000))
    assert _square.mock_calls == []
    assert s[10:15] == [100, 121, 144, 169, 196]
    assert _square.mock_calls == [mocker.call([10, 11, 12, 13, 14])]


def test_take_slice_iterable(mocker):
    mocker.spy(sys.modules[__name__], '_square')
    s = take_slice(_square, range(5))
    assert _square.mock_calls == []
    assert list(s) == [0, 1, 4, 9, 16]
    assert _square.mock_calls == [mocker.call([0, 1, 2, 3, 4])]


def _square(ids):
    return [i ** 2 for i in ids]


@mark.parametrize('path', ['biu', u'a$b'])
def test_check_znode_path_ok(path):
    check_znode_path(path)


@mark.parametrize('path', [
    '..', u'\u0012', 'biu\t', 'biu\n', '/biu', '', ' ', 'a ', 'a/b', 'a\nb',
    'a\rb', 'a\tb', None,
])
def test_check_znode_path_failed(path):
    with raises(ValueError):
        check_znode_path(path)


@mark.parametrize('before,after', [
    ('stable', 'stable'),
    ('stable-altb1', 'stable-altb1'),
    ('stable-altb1-stable', 'stable-altb1-stable'),
    ('altb1', 'altb1'),
    ('altb1-stable', 'altb1-stable'),
    ('altb1-altb1-stable', 'altb1-stable'),
    ('altb1-altb1-altb1-stable', 'altb1-stable'),
    ('altb1-alta1-stable', 'altb1-alta1-stable'),
    ('altb1-altb1-alta1-stable', 'altb1-alta1-stable'),
    ('altb1-alta1-alta1-stable', 'altb1-alta1-alta1-stable'),
])
def test_normalize_cluster_name(mocker, before, after):
    mocker.patch('huskar_api.settings.ROUTE_EZONE_LIST', ['alta1', 'altb1'])
    assert normalize_cluster_name(before) == after


@mark.parametrize('elements,marker,result', [
    ([1, 2, 1], 1, [1, 2, 1]),
    ([1, 1, 2, 1], 1, [1, 2, 1]),
    ([1, 1, 2, 1, 1], 1, [1, 2, 1, 1]),
    ([1, 1], 1, [1]),
    ([], 1, [1]),
])
def test_dedupleft(elements, marker, result):
    assert list(dedupleft(elements, marker)) == result


@mark.xparametrize
def test_merge_instance_list(other_instance_list, user_instance_list, result):
    assert merge_instance_list(
        'test_app', other_instance_list,
        user_instance_list, 'test_cluster') == result


def test_retry_with_max_retry_less_1(mocker):
    momo = mocker.MagicMock()

    def orig_func(*args, **kwargs):
        momo(*args, **kwargs)

    func = retry(Exception, 3, 0)(orig_func)
    func(233)

    momo.assert_not_called()


def test_retry_with_max_retry(mocker):
    momo = mocker.MagicMock()

    def orig_func(*args, **kwargs):
        momo(*args, **kwargs)

    func = retry(Exception, 3, 1)(orig_func)
    func(233)

    momo.assert_called_once()
