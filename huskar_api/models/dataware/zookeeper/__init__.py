from __future__ import absolute_import

from .client import HuskarClient


config_client = HuskarClient('config')
switch_client = HuskarClient('switch')
service_client = HuskarClient('service')
