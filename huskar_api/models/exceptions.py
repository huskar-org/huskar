from __future__ import absolute_import


class HuskarException(Exception):
    """The base class of domain exceptions."""


class NameOccupiedError(HuskarException):
    """The resource name has been occupied."""


class ContainerUnboundError(HuskarException):
    """The container resource has been unbound"""


class MalformedDataError(HuskarException):
    """The data is malformed in upstream."""

    def __init__(self, info, *args, **kwargs):
        super(MalformedDataError, self).__init__(*args, **kwargs)
        self.info = info


class TreeTimeoutError(HuskarException):
    """The initialization of tree holder is timeout."""


class OutOfSyncError(HuskarException):
    """The the local data is outdated."""


class NotEmptyError(HuskarException):
    """The deleting resource is not empty."""


class AuditLogTooLongError(HuskarException):
    """The audit log is too long."""


class AuditLogLostError(HuskarException):
    """The audit log could not be committed to database."""


class EmptyClusterError(HuskarException):
    pass


class InfraNameNotExistError(HuskarException):
    pass


class DataConflictError(HuskarException):
    pass
