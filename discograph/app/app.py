"""
This module defines the core application logic for the Discograph web application.

It sets up the Flask application, configures database connections,
initializes caching, registers API and UI blueprints, and defines error
handling and rate limiting behaviors. It also provides functions for
application initialization and shutdown.

Key components:
    - `create_app`: A factory function for creating the Flask application.
    - `shutdown_application`: A function for gracefully shutting down the application.
    - `init_app`: A function for initializing the application components.
    - Blueprint registration for API and UI.
    - Global error handling for exceptions, 404 errors, and 500 errors.
    - Rate limit header injection.
    - Database session management for Flask.
    - Cache initialization and management.
    - Logging configuration and shutdown.
    - Atexit registration for application shutdown.

The application uses `discograph.config` for configuration settings,
`discograph.app.api` for API endpoints, `discograph.app.ui` for the UI,
`discograph.exceptions` for custom exceptions, `discograph.library.cache.cache_manager`
for caching, `discograph.logging_config` for logging, and
`discograph.runtime.runtime_database_manager` for database management.
"""

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

from discograph.app import api, ui, assets
from discograph.app.assets import create_assets_blueprint
from discograph.config import (
    Configuration,
)
from discograph.exceptions import NotFoundError, BaseError
from discograph.library.cache.cache_manager import CacheManager
from discograph.logging_config import setup_logging, shutdown_logging
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

log = logging.getLogger(__name__)
"""The logger for the application module."""


def create_app(config: Configuration):
    """
    Creates and configures the Flask application.

    This function is a factory for creating the Flask application instance. It
    initializes the application, sets up the database and cache, registers
    blueprints, and configures error handling.

    Args:
        config: The application configuration object.

    Returns:
        Flask: The configured Flask application instance.
    """
    app: Flask = Flask(__name__.split(".")[0])
    """Create a new Flask app"""

    app.config.from_object(config)
    """Loads the configuration object into the app"""

    with app.app_context():
        init_app(config)
    """Initialize the app, with the app context"""

    assets_blueprint = create_assets_blueprint(config)

    app.register_blueprint(api.blueprint, url_prefix="/api")
    """Register the API blueprint"""
    app.register_blueprint(ui.blueprint)
    """Register the UI blueprint"""
    app.register_blueprint(assets_blueprint)
    """Register the Vite assets blueprint"""

    # app.wsgi_app = ProxyFix(app.wsgi_app)
    # Mobility(app)
    Compress(app)
    """Enable gzip compression for all responses"""

    RuntimeDatabaseManager.runtime_database_helper.flask_db_session = scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=RuntimeDatabaseManager.runtime_database_helper.runtime_engine,
        )
    )
    """Create and set the database scoped session to the db helper"""

    # noinspection PyUnusedLocal
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        """
        Shuts down the database session.

        This function is called automatically at the end of each request
        to remove the database session from the context.

        Args:
            exception: An optional exception that may have occurred.
        """
        RuntimeDatabaseManager.runtime_database_helper.flask_db_session.remove()

    @app.after_request
    def inject_rate_limit_headers(response):
        """
        Injects rate limit headers into the response.

        This function adds rate limit headers to the response, if available,
        to inform the client about the current rate limit status.

        Args:
            response: The Flask response object.

        Returns:
            The modified Flask response object.
        """
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
        """
        Handles exceptions in the application.

        This function is a global error handler that catches any unhandled
        exception in the application. It logs the error and returns an
        appropriate JSON response for API requests or an error page for UI
        requests.

        Args:
            error: The exception that occurred.

        Returns:
            A Flask response object with an error message and status code.
        """
        if app.debug:
            log.exception(f"Debug - handle_error() error: {error}")
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
        """
        Handles 404 Not Found errors.

        This function is called when a 404 error occurs. It returns an
        appropriate error page.

        Args:
            error: The error that occurred.

        Returns:
            A Flask response object with an error page and 404 status code.
        """
        error = NotFoundError(message="Not Found")
        rendered_template = render_template("error.html", error=error)
        response = make_response(rendered_template)
        response.status_code = error.status_code
        return response

    # noinspection PyUnusedLocal
    @app.errorhandler(500)
    def handle_error_500(error):
        """
        Handles 500 Server Error errors.

        This function is called when a 500 error occurs. It returns an
        appropriate error page.

        Args:
            error: The error that occurred.

        Returns:
            A Flask response object with an error page and 500 status code.
        """
        error = BaseError(message="Server Error")
        rendered_template = render_template("error.html", error=error)
        response = make_response(rendered_template)
        response.status_code = error.status_code
        return response

    return app


def shutdown_application():
    """
    Shuts down the application.

    This function is called when the application is being shut down. It
    performs cleanup tasks such as closing database connections, shutting
    down the cache, and shutting down logging.
    """
    # Logging may have been shutdown automatically before this point, so we need to reinitialise it again
    setup_logging()
    log.info("######## APPLICATION SHUTDOWN ########")
    RuntimeDatabaseManager.shutdown_database()
    CacheManager.shutdown_cache()
    shutdown_logging()


def init_app(config: Configuration):
    """
    Initializes the application.

    This function initializes the application components, including logging,
    caching, and the database.

    Args:
        config: The application configuration object.
    """
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

    # Shutdown on app exit
    atexit.register(shutdown_application)
