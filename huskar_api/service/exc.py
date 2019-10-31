# TODO Remove this module in future


class HuskarApiException(Exception):
    pass


class ServiceValueError(ValueError):
    """The input value is invalid for service registry."""


class ServiceLinkExisted(HuskarApiException):
    pass


class ServiceLinkError(HuskarApiException):
    pass


class DataExistsError(HuskarApiException):
    message = 'The data you tried to add is already exist.'


class DataNotExistsError(HuskarApiException):
    message = 'The data is not exist.'


class DataNotEmptyError(HuskarApiException):
    message = 'The target is not empty.'


class DuplicatedEZonePrefixError(HuskarApiException):
    message = 'The target should not contain duplicated E-Zone prefix.'


class ClusterNameUnsupportedError(HuskarApiException):
    message = 'Cluster name are not allowed in Huskar.'
