"""
This module defines a production entry point for the Discograph application.

It provides a function, `create_production_app`, to create and configure a
Flask application instance suitable for a production environment. This setup
uses the `SqliteProductionConfiguration` for configuration settings, ensuring
that the application is properly configured for deployment.

Key functionalities include:
    - Creating a Flask application instance with production settings.
    - Loading initial data into the database tables.
    - Returning the configured Flask application instance.

The application configuration is defined in `discograph.config`, and the
database management is handled by `discograph.runtime.runtime_database_manager`.

The `create_app` function from `discograph.app.app` is used to create the
Flask application instance. The `load_tables` method in
`RuntimeDatabaseManager.runtime_database_helper` is used to populate the
database with initial data.

This module is intended to be used as the main entry point for running the
Discograph application in a production environment. It sets up the necessary
components and returns a ready-to-use Flask application.
"""

from flask import Flask

from discograph.app.app import create_app
from discograph.config import SqliteProductionConfiguration
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager


def create_production_app() -> Flask:
    """
    Creates and configures a Flask application for production.

    This function sets up a Flask application instance using the
    `SqliteProductionConfiguration` to ensure it is configured for a
    production environment. It also loads initial data into the database
    tables.

    Returns:
        Flask: A configured Flask application instance ready for production.
    """
    runtime_config = SqliteProductionConfiguration()
    """
    Configuration object for the runtime environment.

    Sets up the configuration for the runtime environment using SQLite,
    suitable for production use.
    """
    app = create_app(runtime_config)
    """
    Flask application instance.

    Creates a new Flask application instance using the specified runtime
    configuration, including settings for the database, cache, and logging.
    """

    # Load data from tables
    RuntimeDatabaseManager.runtime_database_helper.load_tables()
    """
    Loads initial data into the runtime database.

    Populates the tables in the runtime database with initial data,
    such as roles, from the pre-configured data sources.
    """

    return app
