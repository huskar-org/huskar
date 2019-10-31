from huskar_api.service.exc import HuskarApiException


class NoAuthError(HuskarApiException):
    pass


class UserNotExistedError(HuskarApiException):
    pass


class AuthorityNotExistedError(HuskarApiException):
    pass


class LoginError(HuskarApiException):
    pass


class AuthorityExistedError(HuskarApiException):
    pass
