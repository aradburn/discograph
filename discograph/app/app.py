import atexit
import logging
import sys

from flask import Flask
from flask import g
from flask import jsonify
from flask import make_response
from flask import render_template
from flask import request
from flask_compress import Compress
from sqlalchemy.orm import scoped_session, sessionmaker

from discograph.app import api, ui
from discograph.config import (
    Configuration,
)
from discograph.exceptions import NotFoundError, BaseError
from discograph.library.cache.cache_manager import CacheManager
from discograph.logging_config import setup_logging, shutdown_logging
from discograph.runtime.runtime_database.runtime_database_helper import (
    RuntimeDatabaseHelper,
)
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

log = logging.getLogger(__name__)


def create_app(config: Configuration):
    app: Flask = Flask(__name__.split(".")[0])

    app.config.from_object(config)

    with app.app_context():
        init_app(config)

    app.register_blueprint(api.blueprint, url_prefix="/api")
    app.register_blueprint(ui.blueprint)
    # app.wsgi_app = ProxyFix(app.wsgi_app)
    # Mobility(app)
    Compress(app)
    RuntimeDatabaseHelper.flask_db_session = scoped_session(
        sessionmaker(
            autocommit=False, autoflush=False, bind=RuntimeDatabaseHelper.runtime_engine
        )
    )

    # noinspection PyUnusedLocal
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        RuntimeDatabaseHelper.flask_db_session.remove()

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
        else:
            log.warning(f"Error: {error}")
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
        error = BaseError(message="Server Error")
        rendered_template = render_template("error.html", error=error)
        response = make_response(rendered_template)
        response.status_code = error.status_code
        return response

    return app


def shutdown_application():
    pass


def init_app(config: Configuration):
    # Setup logging
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

    log.info(f"Using configuration: {config.__class__.__name__}")

    # Setup cache
    CacheManager.setup_cache(config)
    cache = CacheManager.get_cache()
    print(f"cache: {cache}")
    if cache is None:
        log.error("Cache not set")
        sys.exit()
    else:
        log.debug("Clearing cache")
        CacheManager.clear()

    # Setup Database
    RuntimeDatabaseManager.setup_database(config)

    # RuntimeRoleDataAccess.load_all_roles()
    # RuntimeDatabaseHelper.entity_details_index = (
    #     RuntimeEntityDataAccess.load_entity_details_index_from_file(ENTITY_DETAILS_PATH)
    # )
    # RuntimeDatabaseHelper.text_search_index = (
    #     TextSearchIndex.load_text_search_index_from_file(TEXT_SEARCH_PATH)
    # )

    # Note reverse order (last in first out), logging is the last to be shutdown
    atexit.register(shutdown_logging)
    atexit.register(CacheManager.shutdown_cache)
    atexit.register(RuntimeDatabaseManager.shutdown_database)
