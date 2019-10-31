from __future__ import absolute_import

import gevent.monkey; gevent.monkey.patch_all()  # noqa

from werkzeug.contrib.fixers import ProxyFix
from .app import create_app


app = create_app()
app.wsgi_app = ProxyFix(app.wsgi_app)
