# coding=utf8
from .data import DataBase
from huskar_api.models.dataware.zookeeper import config_client


class ConfigData(DataBase):

    client = config_client
