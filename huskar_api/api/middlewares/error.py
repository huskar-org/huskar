from __future__ import absolute_import

from flask import Blueprint, json
from werkzeug.exceptions import HTTPException, InternalServerError
from marshmallow.exceptions import ValidationError

# TODO Do not use this base exception in future
from huskar_api.service.exc import HuskarApiException
from huskar_api.api.utils import api_response


bp = Blueprint('middlewares.error', __name__)


def http_errorhandler(fn):
    def iter_derived_classes(base_class):
        for class_ in base_class.__subclasses__():
            yield class_
            for derived_class in iter_derived_classes(class_):
                yield derived_class

    for http_error in iter_derived_classes(HTTPException):
        if http_error is InternalServerError:
            continue
        bp.app_errorhandler(http_error)(fn)
    return fn


@http_errorhandler
def handle_http_error(error):
    status = error.name.replace(u' ', '')
    description = error.description

    if isinstance(error, KeyError) and error.args:
        description = u'"%s" is required field.' % error.args[0]

    return api_response(status=status, message=description), error.code


@bp.app_errorhandler(HuskarApiException)
def handle_huskar_api_error(error):
    status = error.__class__.__name__
    description = (
        next(iter(error.args), None) or getattr(error, 'message', None) or u'')
    return api_response(status=status, message=description), 400


@bp.app_errorhandler(ValidationError)
def handle_marshmallow_validation_error(error):
    description = json.dumps(error.messages)
    return api_response(status='ValidationError', message=description), 400


@bp.app_errorhandler(InternalServerError)
def handle_unknown_error(error):
    return handle_http_error(InternalServerError())
