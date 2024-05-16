import collections
import enum
import logging
import os
import tempfile
from copy import deepcopy

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


class Configuration(collections.abc.Mapping):
    def __init__(self, data):
        config_data = {
            key: value
            for key, value in data.items()
            if not key.startswith("_") and not callable(key)
        }
        self._data = deepcopy(config_data)

    def __getitem__(self, key):
        return self._data[key]

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)


class PostgresProductionConfiguration(Configuration):
    PRODUCTION = True
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_USERNAME = os.getenv("DISCOGRAPH_DATABASE_USERNAME")
    POSTGRES_DATABASE_PASSWORD = os.getenv("DISCOGRAPH_DATABASE_PASSWORD")
    POSTGRES_DATABASE_HOST = os.getenv("DISCOGRAPH_DATABASE_HOST")
    POSTGRES_DATABASE_PORT = os.getenv("DISCOGRAPH_DATABASE_PORT")
    POSTGRES_DATABASE_NAME = os.getenv("DISCOGRAPH_DATABASE_NAME")
    APPLICATION_ROOT = "https://discograph.azurewebsites.net/"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.FILESYSTEM

    def __init__(self):
        super().__init__(vars(PostgresProductionConfiguration))


class PostgresDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_USERNAME = "discograph"
    POSTGRES_DATABASE_PASSWORD = "discograph"
    POSTGRES_DATABASE_HOST = "localhost"
    POSTGRES_DATABASE_PORT = 5432
    POSTGRES_DATABASE_NAME = "discograph_dev"
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.REDIS

    def __init__(self):
        super().__init__(vars(PostgresDevelopmentConfiguration))


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

    def __init__(self):
        super().__init__(vars(PostgresTestConfiguration))


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

    def __init__(self):
        super().__init__(vars(SqliteDevelopmentConfiguration))


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

    def __init__(self):
        super().__init__(vars(SqliteTestConfiguration))


class CockroachDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.COCKROACH
    COCKROACH_DATABASE_NAME = "dev_discograph"
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.FILESYSTEM

    def __init__(self):
        super().__init__(vars(CockroachDevelopmentConfiguration))


class CockroachTestConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.COCKROACH
    COCKROACH_DATABASE_NAME = "test_discograph"
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.MEMORY

    def __init__(self):
        super().__init__(vars(CockroachTestConfiguration))
