from flask import Flask

from discograph.app.app import create_app
from discograph.config import PostgresProductionConfiguration
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager


def create_production_app() -> Flask:
    runtime_config = PostgresProductionConfiguration()
    app = create_app(runtime_config)

    # Load data from tables
    RuntimeDatabaseManager.runtime_db_helper.load_tables()

    return app
