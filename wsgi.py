import atexit

from discograph.app import app, setup_application
from discograph.database import setup_database, shutdown_database

setup_application()
setup_database(app.config)
atexit.register(shutdown_database)
