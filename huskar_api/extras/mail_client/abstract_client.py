import abc


class AbstractMailClient(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def deliver_email(self, receiver, subject, message, cc):
        pass  # pragma: no cover
