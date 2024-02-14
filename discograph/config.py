import enum
import logging
import os
import tempfile

from dotenv import load_dotenv, find_dotenv, dotenv_values

log = logging.getLogger(__name__)

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(APP_DIR, ".."))
LOGGING_DIR = os.path.join(ROOT_DIR, "logs")
LOGGING_FILE = os.path.join(LOGGING_DIR, "discograph.log")
LOGGING_ERROR_FILE = os.path.join(LOGGING_DIR, "error.log")
LOGGING_DEBUG_FILE = os.path.join(LOGGING_DIR, "debug.log")

env_file = find_dotenv()
env_config = dotenv_values()  # take environment variables from .env.
load_dotenv(override=True, verbose=True)  # take environment variables from .env.


class DatabaseType(enum.Enum):
    POSTGRES = 1
    SQLITE = 2
    COCKROACH = 3


class ThreadingModel(enum.Enum):
    PROCESS = 1
    THREAD = 2


class CacheType(enum.Enum):
    MEMORY = 1
    FILESYSTEM = 2
    REDIS = 3


class Configuration(object):
    pass


class PostgresProductionConfiguration(Configuration):
    log.info("PostgresProductionConfiguration")
    log.info(f"DATABASE_HOST: {os.getenv('DATABASE_HOST')}")
    log.info(f"DATABASE_NAME: {os.getenv('DATABASE_NAME')}")
    PRODUCTION = True
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_USERNAME = os.getenv("DATABASE_USERNAME")
    POSTGRES_DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
    POSTGRES_DATABASE_HOST = os.getenv("DATABASE_HOST")
    POSTGRES_DATABASE_PORT = os.getenv("DATABASE_PORT")
    POSTGRES_DATABASE_NAME = os.getenv("DATABASE_NAME")
    APPLICATION_ROOT = "https://discograph.azurewebsites.net/"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.FILESYSTEM


class PostgresDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = "dev_discograph"
    POSTGRES_ROOT = "/usr/lib/postgresql/16"
    POSTGRES_DATA = "/data1/tmp/pg_temp/dev"
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.FILESYSTEM


class PostgresTestConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = "test_discograph"
    POSTGRES_ROOT = "/usr/lib/postgresql/16"
    POSTGRES_DATA = "/data1/tmp/pg_temp/test"
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.MEMORY


class SqliteDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.SQLITE
    SQLITE_DATABASE_NAME = os.path.join(
        tempfile.gettempdir(), "discograph", "discograph.db"
    )
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.THREAD
    CACHE_TYPE = CacheType.FILESYSTEM


class SqliteTestConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.SQLITE
    SQLITE_DATABASE_NAME = os.path.join(
        tempfile.gettempdir(), "discograph", "test_discograph.db"
    )
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.THREAD
    CACHE_TYPE = CacheType.MEMORY


class CockroachDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.COCKROACH
    COCKROACH_DATABASE_NAME = "dev_discograph"
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.FILESYSTEM


class CockroachTestConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.COCKROACH
    COCKROACH_DATABASE_NAME = "test_discograph"
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.MEMORY
