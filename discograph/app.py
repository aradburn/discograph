import atexit
import os
import traceback

import fakeredis
from flask import Flask
from flask import g
from flask import jsonify
from flask import make_response
from flask import render_template
from flask import request
from flask_caching.backends.filesystemcache import FileSystemCache
from flask_caching.backends.rediscache import RedisCache
from flask_compress import Compress
from flask_mobility import Mobility
from werkzeug.middleware.proxy_fix import ProxyFix

from discograph import api
from discograph import exceptions
from discograph import ui
from discograph.helpers import setup_database, shutdown_database

app: Flask = Flask(__name__)
app_file_cache: FileSystemCache
app_redis_cache: RedisCache


def setup_application():
    global app, app_file_cache, app_redis_cache

    app.config.from_object('discograph.config.PostgresDevelopmentConfiguration')
    # app.config.from_object('discograph.config.SqliteDevelopmentConfiguration')
    # TODO AJR app.config.from_object('discograph.locals')

    app_file_cache = FileSystemCache(
        app.config['FILE_CACHE_PATH'],
        default_timeout=app.config['FILE_CACHE_TIMEOUT'],
        threshold=app.config['FILE_CACHE_THRESHOLD'],
    )
    if not os.path.exists(app.config['FILE_CACHE_PATH']):
        os.makedirs(app.config['FILE_CACHE_PATH'])
    app_redis_cache = fakeredis.FakeRedis()
    # app_redis_cache = RedisCache()
    app.register_blueprint(api.blueprint, url_prefix='/api')
    app.register_blueprint(ui.blueprint)
    app.wsgi_app = ProxyFix(app.wsgi_app)
    Mobility(app)
    Compress(app)


def shutdown_application():
    global app, app_file_cache, app_redis_cache
    app = Flask(__name__)
    app_file_cache = None
    app_redis_cache = None


@app.after_request
def inject_rate_limit_headers(response):
    try:
        requests, remaining, reset = map(int, g.view_limits)
    except (AttributeError, ValueError):
        return response
    else:
        h = response.headers
        h.add('X-RateLimit-Remaining', remaining)
        h.add('X-RateLimit-Limit', requests)
        h.add('X-RateLimit-Reset', reset)
        return response


@app.errorhandler(Exception)
def handle_error(error):
    if app.debug:
        traceback.print_exc()
    status_code = getattr(error, 'status_code', 400)
    if request.endpoint.startswith('api'):
        response = jsonify({
            'success': False,
            'status': status_code,
            'message': getattr(error, 'message', 'Error')
            })
    else:
        rendered_template = render_template('error.html', error=error)
        response = make_response(rendered_template)
    response.status_code = status_code
    return response


# noinspection PyUnusedLocal
@app.errorhandler(404)
def handle_error_404(error):
    status_code = 404
    error = exceptions.APIError(
        message='Not Found',
        status_code=status_code,
        )
    rendered_template = render_template('error.html', error=error)
    response = make_response(rendered_template)
    response.status_code = status_code
    return response


# noinspection PyUnusedLocal
@app.errorhandler(500)
def handle_error_500(error):
    status_code = 500
    error = exceptions.APIError(
        message='Something Broke',
        status_code=status_code,
        )
    rendered_template = render_template('error.html', error=error)
    response = make_response(rendered_template)
    response.status_code = status_code
    return response


if __name__ == '__main__':
    setup_application()
    setup_database(app.config)
    atexit.register(shutdown_database)
    app.run(debug=False)
