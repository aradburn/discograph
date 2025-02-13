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
from werkzeug.middleware.proxy_fix import ProxyFix

from discograph import api
from discograph import ui
from discograph.config import (
    SqliteDevelopmentConfiguration,
    ENTITY_DETAILS_PATH,
    TEXT_SEARCH_PATH,
)
from discograph.exceptions import NotFoundError, BaseError
from discograph.library.cache.cache_manager import CacheManager
from discograph.library.full_text_search.text_search_index import TextSearchIndex
from discograph.logging_config import setup_logging, shutdown_logging
from discograph.runtime.data_access_layer.runtime_entity_data_access import (
    RuntimeEntityDataAccess,
)
from discograph.runtime.data_access_layer.runtime_role_data_access import (
    RuntimeRoleDataAccess,
)
from discograph.runtime.runtime_database.runtime_database_helper import (
    RuntimeDatabaseHelper,
)
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

log = logging.getLogger(__name__)

app: Flask = Flask(__name__.split(".")[0])
# app: Flask = Flask(__name__)


def setup_application():
    global app

    app.register_blueprint(api.blueprint, url_prefix="/api")
    app.register_blueprint(ui.blueprint)
    app.wsgi_app = ProxyFix(app.wsgi_app)
    # Mobility(app)
    Compress(app)
    RuntimeDatabaseHelper.flask_db_session = scoped_session(
        sessionmaker(
            autocommit=False, autoflush=False, bind=RuntimeDatabaseHelper.engine
        )
    )


def shutdown_application():
    global app
    app = Flask(__name__)


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
    log.info("Using SqliteDevelopmentConfiguration")
    runtime_config = SqliteDevelopmentConfiguration()
    # config = vars(PostgresDevelopmentConfiguration)
    app.config.from_object(runtime_config)
    CacheManager.setup_cache(runtime_config)
    cache = CacheManager.get_cache()
    print(f"cache: {cache}")
    if cache is None:
        log.error("Cache not set")
        sys.exit()
    else:
        log.debug("Clearing cache")
        CacheManager.clear()

    # Setup Database
    RuntimeDatabaseManager.setup_database(runtime_config)

    # Setup Application
    setup_application()

    RuntimeRoleDataAccess.load_all_roles()
    RuntimeDatabaseHelper.entity_details_index = (
        RuntimeEntityDataAccess.load_entity_details_index_from_file(ENTITY_DETAILS_PATH)
    )
    RuntimeDatabaseHelper.text_search_index = (
        TextSearchIndex.load_text_search_index_from_file(TEXT_SEARCH_PATH)
    )

    # Note reverse order (last in first out), logging is the last to be shutdown
    atexit.register(shutdown_logging)
    atexit.register(CacheManager.shutdown_cache)
    atexit.register(RuntimeDatabaseManager.shutdown_database)


if __name__ == "__main__":
    # Flask development server, not to be used in production
    main()
    app.run(debug=False)
