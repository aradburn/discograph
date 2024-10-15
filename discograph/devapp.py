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
from sqlalchemy.orm import scoped_session, sessionmaker
from werkzeug.middleware.proxy_fix import ProxyFix

from discograph import api
from discograph import ui
from discograph.config import (
    PostgresDevelopmentConfiguration,
)
from discograph.database import setup_database, shutdown_database
from discograph.exceptions import NotFoundError, BaseError
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.library.database.database_helper import DatabaseHelper
from discograph.logging_config import setup_logging, shutdown_logging

log = logging.getLogger(__name__)

app: Flask = Flask(__name__.split(".")[0])
# app: Flask = Flask(__name__)


def setup_application():
    global app

    app.register_blueprint(api.blueprint, url_prefix="/api")
    app.register_blueprint(ui.blueprint)
    app.wsgi_app = ProxyFix(app.wsgi_app)
    Mobility(app)
    Compress(app)
    DatabaseHelper.flask_db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=DatabaseHelper.engine)
    )


def shutdown_application():
    global app
    app = Flask(__name__)


# noinspection PyUnusedLocal
@app.teardown_appcontext
def shutdown_session(exception=None):
    DatabaseHelper.flask_db_session.remove()


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
    error = NotFoundError(message="Not Found")
    rendered_template = render_template("error.html", error=error)
    response = make_response(rendered_template)
    response.status_code = error.status_code
    return response


# noinspection PyUnusedLocal
@app.errorhandler(500)
def handle_error_500(error):
    error = BaseError(message="Something Broke")
    rendered_template = render_template("error.html", error=error)
    response = make_response(rendered_template)
    response.status_code = error.status_code
    return response


def main():
    setup_logging()
    log.info("")
    log.info("")
    log.info("######  #   # #   ####   ####   ####   ####    ##   #####  #    # ")
    log.info("#     # # #      #    # #    # #    # #    #  #  #  #    # #    # ")
    log.info("#     # #  ####  #      #    # #      #    # #    # #    # ###### ")
    log.info("#     # #      # #      #    # #  ### #####  ###### #####  #    # ")
    log.info("#     # # #    # #    # #    # #    # #   #  #    # #      #    # ")
    log.info("######  #  ####   ####   ####   ####  #    # #    # #      #    # ")
    log.info("")
    log.info("")
    log.info("Using PostgresDevelopmentConfiguration")
    config = vars(PostgresDevelopmentConfiguration)
    app.config.from_object(config)
    setup_cache(config)
    setup_database(config)
    setup_application()
    # Note reverse order (last in first out), logging is the last to be shutdown
    atexit.register(shutdown_logging)
    atexit.register(shutdown_cache)
    atexit.register(shutdown_database, config)


if __name__ == "__main__":
    # Flask development server, not to be used in production
    main()
    app.run(debug=False)
