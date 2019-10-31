# coding=utf8
from huskar_api.models.dataware.zookeeper import switch_client

from .data import DataBase


class SwitchData(DataBase):

    client = switch_client
