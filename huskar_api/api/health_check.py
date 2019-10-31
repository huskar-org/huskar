from __future__ import absolute_import

from flask.views import MethodView

from .utils import api_response


class HealthCheckView(MethodView):
    def get(self):
        return api_response('ok')
