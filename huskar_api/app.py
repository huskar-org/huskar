from __future__ import absolute_import

from flask import Flask
from werkzeug.utils import import_string

from huskar_api import settings


extensions = [
    'huskar_api.ext:babel',
    'huskar_api.ext:sentry',
    'huskar_api.ext:db_tester',
]

blueprints = [
    ('huskar_api.api.middlewares.rate_limit_ip:bp', None),
    ('huskar_api.api.middlewares.auth:bp', None),
    ('huskar_api.api.middlewares.concurrent_limit:bp', None),
    ('huskar_api.api.middlewares.rate_limit_user:bp', None),
    ('huskar_api.api.middlewares.route:bp', None),
    ('huskar_api.api.middlewares.error:bp', None),
    ('huskar_api.api.middlewares.db:bp', None),
    ('huskar_api.api.middlewares.logger:bp', None),
    ('huskar_api.api.middlewares.read_only:bp', None),
    ('huskar_api.api.middlewares.control_access_via_api:bp', None),

    ('huskar_api.api:bp', '/api'),
]


def create_app():
    app = Flask(__name__)
    app.config['DEBUG'] = settings.DEBUG
    app.config['SECRET_KEY'] = settings.SECRET_KEY
    app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = False
    app.config['SENTRY_DSN'] = settings.SENTRY_DSN
    app.config['BABEL_DEFAULT_LOCALE'] = settings.DEFAULT_LOCALE
    app.config['BABEL_DEFAULT_TIMEZONE'] = settings.DEFAULT_TIMEZONE
    app.config['LOGGER_HANDLER_POLICY'] = 'never'
    app.logger.propagate = True

    for extension_qualname in extensions:
        extension = import_string(extension_qualname)
        extension.init_app(app)

    for blueprint_qualname, url_prefix in blueprints:
        blueprint = import_string(blueprint_qualname)
        app.register_blueprint(blueprint, url_prefix=url_prefix)

    return app
