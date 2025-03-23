"""
This module provides a development entry point for the Discograph application.

It sets up and runs a local development server using Flask, utilizing the
SQLite database for data storage and retrieval. It's intended for development
and testing purposes and should not be used in a production environment.

Key functionalities include:
    - Creating a Flask application instance with SQLite configuration.
    - Loading initial data into the database tables.
    - Starting the Flask development server.

The application configuration is defined in `discograph.config`, and the
database management is handled by `discograph.runtime.runtime_database_manager`.

The `create_app` function in `discograph.app.app` is used to create the
Flask application instance. The `load_tables` method in
`RuntimeDatabaseManager.runtime_database_helper` is used to populate the
database with initial data.
"""

from discograph.app.app import create_app
from discograph.config import SqliteDevelopmentConfiguration
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

if __name__ == "__main__":
    # Flask development server, not to be used in production
    runtime_config = SqliteDevelopmentConfiguration()
    """
    Configuration object for the runtime environment.

    Sets up the configuration for the runtime environment using SQLite.
    """
    app = create_app(runtime_config)
    """
    Flask application instance.

    Creates a new Flask application instance using the specified runtime
    configuration.
    """

    # Load data from tables
    RuntimeDatabaseManager.runtime_database_helper.load_tables()
    """
    Loads initial data into the runtime database.

    Populates the tables in the runtime database with initial data,
    such as roles, from the pre-configured data sources.
    """

    app.run(debug=False)
    """
    Starts the Flask development server.

    Runs the Flask development server, which listens for incoming HTTP
    requests and serves the Discograph application. The `debug=False`
    setting disables debug mode.
    """
