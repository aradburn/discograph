import atexit
import logging

from flask import Flask
from flask import g
from flask import jsonify
from flask import make_response
from flask import render_template
from flask import request
from flask_compress import Compress
from flask_mobility import Mobility
from werkzeug.middleware.proxy_fix import ProxyFix

from discograph import api
from discograph import exceptions
from discograph import ui
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.database import setup_database, shutdown_database
from discograph.logging import setup_logging, shutdown_logging

log = logging.getLogger(__name__)

app: Flask = Flask(__name__.split(".")[0])
# app: Flask = Flask(__name__)


def setup_application():
    global app

    # app.config.from_object('discograph.config.CockroachDevelopmentConfiguration')
    # app.config.from_object('discograph.config.PostgresDevelopmentConfiguration')
    # app.config.from_object("discograph.config.PostgresProductionConfiguration")
    # app.config.from_object('discograph.config.SqliteDevelopmentConfiguration')

    app.register_blueprint(api.blueprint, url_prefix="/api")
    app.register_blueprint(ui.blueprint)
    app.wsgi_app = ProxyFix(app.wsgi_app)
    Mobility(app)
    Compress(app)


def shutdown_application():
    global app
    app = Flask(__name__)


@app.after_request
def inject_rate_limit_headers(response):
    try:
        requests, remaining, reset = map(int, g.view_limits)
    except (AttributeError, ValueError):
        return response
    else:
        h = response.headers
        h.add("X-RateLimit-Remaining", remaining)
        h.add("X-RateLimit-Limit", requests)
        h.add("X-RateLimit-Reset", reset)
        return response


@app.errorhandler(Exception)
def handle_error(error):
    if app.debug:
        log.exception(error)
    status_code = getattr(error, "status_code", 400)
    if request.endpoint.startswith("api"):
        response = jsonify(
            {
                "success": False,
                "status": status_code,
                "message": getattr(error, "message", "Error"),
            }
        )
    else:
        rendered_template = render_template("error.html", error=error)
        response = make_response(rendered_template)
    response.status_code = status_code
    return response


# noinspection PyUnusedLocal
@app.errorhandler(404)
def handle_error_404(error):
    status_code = 404
    error = exceptions.APIError(
        message="Not Found",
        status_code=status_code,
    )
    rendered_template = render_template("error.html", error=error)
    response = make_response(rendered_template)
    response.status_code = status_code
    return response


# noinspection PyUnusedLocal
@app.errorhandler(500)
def handle_error_500(error):
    status_code = 500
    error = exceptions.APIError(
        message="Something Broke",
        status_code=status_code,
    )
    rendered_template = render_template("error.html", error=error)
    response = make_response(rendered_template)
    response.status_code = status_code
    return response


def main():
    setup_logging()
    setup_cache(app.config)
    setup_database(app.config)
    setup_application()
    # Note reverse order (last in first out), logging is the last to be shutdown
    atexit.register(shutdown_logging)
    atexit.register(shutdown_cache)
    atexit.register(shutdown_database)


if __name__ == "__main__":
    # Flask development server, not to be used in production
    main()
    app.run(debug=False)
