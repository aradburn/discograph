import enum
import os
import tempfile
from dotenv import load_dotenv, find_dotenv, dotenv_values

env_file = find_dotenv()
print(f"Using {env_file} env")
env_config = dotenv_values()  # take environment variables from .env.
load_dotenv(override=True, verbose=True)  # take environment variables from .env.
print(f"os env: {os.environ}")
print(f"env_config: {env_config}")
print(f"os.getenv('DATABASE_USERNAME'): {os.getenv('DATABASE_USERNAME')}")


class DatabaseType(enum.Enum):
    POSTGRES = 1
    SQLITE = 2
    COCKROACH = 3


class ThreadingModel(enum.Enum):
    PROCESS = 1
    THREAD = 2


class Configuration(object):
    FILE_CACHE_PATH = os.path.join(tempfile.gettempdir(), 'discograph', 'cache')
    FILE_CACHE_THRESHOLD = 1024 * 128
    FILE_CACHE_TIMEOUT = 60 * 60 * 24 * 7


class PostgresProductionConfiguration(Configuration):
    PRODUCTION = True
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_USERNAME = os.getenv("DATABASE_USERNAME")
    POSTGRES_DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
    POSTGRES_DATABASE_HOST = os.getenv("DATABASE_HOST")
    POSTGRES_DATABASE_PORT = os.getenv("DATABASE_PORT")
    POSTGRES_DATABASE_NAME = os.getenv("DATABASE_NAME")
    APPLICATION_ROOT = 'http://discograph.org'
    THREADING_MODEL = ThreadingModel.PROCESS


class PostgresDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = 'dev_discograph'
    POSTGRES_ROOT = '/usr/lib/postgresql/16'
    POSTGRES_DATA = '/data1/tmp/pg_temp/dev'
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.PROCESS


class PostgresTestConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = 'test_discograph'
    POSTGRES_ROOT = '/usr/lib/postgresql/16'
    POSTGRES_DATA = '/data1/tmp/pg_temp/test'
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.PROCESS


class SqliteDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.SQLITE
    SQLITE_DATABASE_NAME = os.path.join(tempfile.gettempdir(), 'discograph', 'discograph.db')
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.THREAD


class SqliteTestConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.SQLITE
    SQLITE_DATABASE_NAME = os.path.join(tempfile.gettempdir(), 'discograph', 'test_discograph.db')
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.THREAD


class CockroachDevelopmentConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.COCKROACH
    COCKROACH_DATABASE_NAME = 'dev_discograph'
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.PROCESS


class CockroachTestConfiguration(Configuration):
    PRODUCTION = False
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.COCKROACH
    COCKROACH_DATABASE_NAME = 'test_discograph'
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.PROCESS


__all__ = [
    'DatabaseType',
    'ThreadingModel',
    'PostgresProductionConfiguration',
    'PostgresDevelopmentConfiguration',
    'PostgresTestConfiguration',
    'SqliteDevelopmentConfiguration',
    'SqliteTestConfiguration',
    'CockroachDevelopmentConfiguration',
    'CockroachTestConfiguration',
]
