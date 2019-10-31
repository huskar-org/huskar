from __future__ import absolute_import

import copy

from pytest import raises

from huskar_api.models.audit import action_types, action_creator


def test_action_types():
    assert action_types.CREATE_TEAM == 1001
    assert action_types[1001] == 'CREATE_TEAM'

    with raises(AttributeError):
        action_types.DISCARD_TYPE
    with raises(AttributeError):
        action_types._DISCARD_TYPE
    with raises(KeyError):
        action_types[-1]

    with raises(AttributeError):
        action_types.create_team
    with raises(KeyError):
        action_types['1001']

    with raises(AttributeError):
        action_types.CREATE_TEAM = 1001


def test_action_creator():
    creator = copy.deepcopy(action_creator)

    @creator(10010)
    def make_china_unicom(action_type, telephone):
        return {'telephone': telephone}, []

    with raises(KeyError):
        creator.make_action(10086)

    with raises(TypeError):
        creator.make_action(10010)

    action = creator.make_action(10010, telephone='10010')
    assert len(action) == 3
    assert action[0] == 10010
    assert action[1] == {'telephone': '10010'}
    assert action[2] == []
