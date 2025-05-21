import collections
import enum
import logging
import os
from copy import deepcopy
from pathlib import Path

from dotenv import load_dotenv, find_dotenv, dotenv_values

from discograph import utils

log = logging.getLogger(__name__)

# KEYS
PRODUCTION_KEY = "PRODUCTION"
DEBUG_KEY = "DEBUG"
TESTING_KEY = "TESTING"
DATA_DIR_KEY = "DATA_DIR"
DATABASE_KEY = "DATABASE"
POSTGRES_DATABASE_USERNAME_KEY = "POSTGRES_DATABASE_USERNAME"
POSTGRES_DATABASE_PASSWORD_KEY = "POSTGRES_DATABASE_PASSWORD"
POSTGRES_DATABASE_HOST_KEY = "POSTGRES_DATABASE_HOST"
POSTGRES_DATABASE_PORT_KEY = "POSTGRES_DATABASE_PORT"
POSTGRES_OFFLINE_DATABASE_NAME_KEY = "POSTGRES_OFFLINE_DATABASE_NAME"
POSTGRES_RUNTIME_DATABASE_NAME_KEY = "POSTGRES_RUNTIME_DATABASE_NAME"
POSTGRES_ROOT_KEY = "POSTGRES_ROOT"
POSTGRES_OFFLINE_DATA_KEY = "POSTGRES_OFFLINE_DATA"
POSTGRES_RUNTIME_DATA_KEY = "POSTGRES_RUNTIME_DATA"
APPLICATION_ROOT_KEY = "APPLICATION_ROOT"
THREADING_MODEL_KEY = "THREADING_MODEL"
CACHE_TYPE_KEY = "CACHE_TYPE"
SQLITE_OFFLINE_DATABASE_NAME_KEY = "SQLITE_OFFLINE_DATABASE_NAME"
SQLITE_RUNTIME_DATABASE_NAME_KEY = "SQLITE_RUNTIME_DATABASE_NAME"

APP_DIR = Path(__file__).parent.resolve()
ROOT_DIR = Path(APP_DIR / "..").resolve()
FRONTEND_DIR = ROOT_DIR / "frontend"
TEMPLATES_DIR = FRONTEND_DIR / "templates"
PUBLIC_DIR = FRONTEND_DIR / "public"

# DATA
DISCOGS_DATA = "discogs"
ROLES_DATA = "roles"
INSTRUMENTS_DATA = "instruments"
INSTRUMENTS_DATA_FILENAMES = [
    "aerophones.csv",
    "chordophones.csv",
    "electrophones.csv",
    "idiophones.csv",
    "membranophones.csv",
]
HS_INSTRUMENTS_FILENAME = "hornbostel_sachs.json"
TEXT_SEARCH_DATA = "text_search"
TEXT_SEARCH_FILENAME = "text_search.data"
ENTITY_DETAILS_DATA = "entity_details"
ENTITY_DETAILS_FILENAME = "entity_details.data"

# DATABASE
OFFLINE_DATABASE = "offline_database"
RUNTIME_DATABASE = "runtime_database"

TEST_DIR = ROOT_DIR / "tests"


# TEST_TEXT_SEARCH_DIR = os.path.join(ROOT_DIR, "tests", "data_text_search")
# TEST_TEXT_SEARCH_PATH = Path(TEST_TEXT_SEARCH_DIR, "text_search.data")

LOGGING_DIR = ROOT_DIR / "logs"
LOGGING_FILE = LOGGING_DIR / "discograph.log"
LOGGING_ERROR_FILE = LOGGING_DIR / "error.log"
LOGGING_DEBUG_FILE = LOGGING_DIR / "debug.log"

DISCOGS_BASE_URL = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/{year}/"
DISCOGS_FILE_TEMPLATE = "discogs_{date}_{type}.xml.gz"
DISCOGS_ARTISTS_TYPE = "artists"
DISCOGS_RELEASES_TYPE = "releases"
DISCOGS_LABELS_TYPE = "labels"
DISCOGS_MASTERS_TYPE = "masters"

ALL_OFFLINE_DATABASE_TABLE_NAMES = [
    "entity",
    "relation",
    "release",
    "role",
    "relation_release_year",
    "metadata",
]
ALL_RUNTIME_DATABASE_TABLE_NAMES = [
    "runtime_entity",
    "runtime_relation",
    "runtime_role",
]
OFFLINE_DATABASE_TABLE_NAMES_WITHOUT_ROLE = [
    "relation_release_year",
    "relation",
    "entity",
    "release",
    "metadata",
]
RUNTIME_DATABASE_TABLE_NAMES_WITHOUT_ROLE = [
    "runtime_relation",
    "runtime_entity",
]

env_file = find_dotenv()
env_config = dotenv_values()  # take environment variables from .env.
load_dotenv(override=True, verbose=True)  # take environment variables from .env.


class DatabaseType(enum.Enum):
    POSTGRES = 1
    SQLITE = 2


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
    DATA_DIR = ROOT_DIR / "discograph" / "data"
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_USERNAME = os.getenv("DISCOGRAPH_DATABASE_USERNAME")
    POSTGRES_DATABASE_PASSWORD = os.getenv("DISCOGRAPH_DATABASE_PASSWORD")
    POSTGRES_DATABASE_HOST = os.getenv("DISCOGRAPH_DATABASE_HOST")
    POSTGRES_DATABASE_PORT = os.getenv("DISCOGRAPH_DATABASE_PORT")
    POSTGRES_OFFLINE_DATABASE_NAME = os.getenv("DISCOGRAPH_DATABASE_NAME")
    APPLICATION_ROOT = "https://discograph.azurewebsites.net/"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.FILESYSTEM

    def __init__(self):
        super().__init__(vars(PostgresProductionConfiguration))


class PostgresDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATA_DIR = ROOT_DIR / "discograph" / "data"
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_USERNAME = "discograph"
    POSTGRES_DATABASE_PASSWORD = "discograph"
    POSTGRES_DATABASE_HOST = "localhost"
    POSTGRES_DATABASE_PORT = 5432
    POSTGRES_OFFLINE_DATABASE_NAME = "discograph_dev"
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.REDIS

    def __init__(self):
        super().__init__(vars(PostgresDevelopmentConfiguration))


class PostgresTestConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = True
    DATA_DIR = TEST_DIR / "data"
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_OFFLINE_DATABASE_NAME = "test_offline_discograph"
    POSTGRES_RUNTIME_DATABASE_NAME = "test_runtime_discograph"
    POSTGRES_ROOT = "/usr/lib/postgresql/17"
    POSTGRES_OFFLINE_DATA = TEST_DIR / OFFLINE_DATABASE
    POSTGRES_RUNTIME_DATA = TEST_DIR / RUNTIME_DATABASE
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.PROCESS
    CACHE_TYPE = CacheType.MEMORY

    def __init__(self):
        super().__init__(vars(PostgresTestConfiguration))


class SqliteProductionConfiguration(Configuration):
    PRODUCTION = True
    DEBUG = False
    TESTING = False
    DATA_DIR = ROOT_DIR / "discograph" / "data"
    DATABASE = DatabaseType.SQLITE
    SQLITE_OFFLINE_DATABASE_NAME = (
        ROOT_DIR / OFFLINE_DATABASE / "discograph_offline_prod.db"
    )
    SQLITE_RUNTIME_DATABASE_NAME = (
        ROOT_DIR / RUNTIME_DATABASE / "discograph_runtime_prod.db"
    )
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.THREAD
    CACHE_TYPE = CacheType.FILESYSTEM

    def __init__(self):
        super().__init__(vars(SqliteProductionConfiguration))


class SqliteDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATA_DIR = ROOT_DIR / "discograph" / "data"
    DATABASE = DatabaseType.SQLITE
    SQLITE_OFFLINE_DATABASE_NAME = (
        ROOT_DIR / OFFLINE_DATABASE / "discograph_offline_dev.db"
    )
    SQLITE_RUNTIME_DATABASE_NAME = (
        ROOT_DIR / RUNTIME_DATABASE / "discograph_runtime_dev.db"
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
    DATA_DIR = TEST_DIR / "data"
    DATABASE = DatabaseType.SQLITE
    SQLITE_OFFLINE_DATABASE_NAME = (
        TEST_DIR
        / OFFLINE_DATABASE
        / ("discograph_offline_" + utils.get_random_string(5) + "_test.db")
    )
    SQLITE_RUNTIME_DATABASE_NAME = (
        TEST_DIR
        / RUNTIME_DATABASE
        / ("discograph_runtime_" + utils.get_random_string(5) + "_test.db")
    )
    APPLICATION_ROOT = "http://localhost"
    THREADING_MODEL = ThreadingModel.THREAD
    CACHE_TYPE = CacheType.MEMORY

    def __init__(self):
        super().__init__(vars(SqliteTestConfiguration))
