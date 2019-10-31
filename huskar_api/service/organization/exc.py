from huskar_api.service.exc import HuskarApiException


class ApplicationNotExistedError(HuskarApiException):
    pass


class ApplicationExistedError(HuskarApiException):
    pass
