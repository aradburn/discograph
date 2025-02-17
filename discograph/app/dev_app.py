from discograph.app.app import create_app
from discograph.config import SqliteDevelopmentConfiguration
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

if __name__ == "__main__":
    # Flask development server, not to be used in production
    runtime_config = SqliteDevelopmentConfiguration()
    app = create_app(runtime_config)

    # Load data from tables
    RuntimeDatabaseManager.runtime_db_helper.load_tables()

    app.run(debug=False)
