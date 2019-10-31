from __future__ import absolute_import


class MonitorClient(object):
    def __init__(self):
        pass

    def timing(self, name, time, tags=None, upper_enable=True):
        pass

    def increment(self, name, sample_rate=1, tags=None):
        pass

    def payload(self, name, data_length=0, tags=None):
        pass


monitor_client = MonitorClient()
