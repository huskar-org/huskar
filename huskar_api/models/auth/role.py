from __future__ import absolute_import

from enum import Enum, unique


@unique
class Authority(Enum):
    READ = u'read'
    WRITE = u'write'
    ADMIN = u'admin'
