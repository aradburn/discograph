import enum
import os
import tempfile


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
    DEBUG = False
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = 'discograph'
    POSTGRES_ROOT = '/usr/lib/postgresql/14'
    APPLICATION_ROOT = 'http://discograph.org'
    THREADING_MODEL = ThreadingModel.PROCESS


class PostgresDevelopmentConfiguration(Configuration):
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = 'dev_discograph'
    POSTGRES_ROOT = '/usr/lib/postgresql/14'
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.PROCESS


class SqliteDevelopmentConfiguration(Configuration):
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.SQLITE
    SQLITE_DATABASE_NAME = os.path.join(tempfile.gettempdir(), 'discograph', 'discograph.db')
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.THREAD


class PostgresTestConfiguration(Configuration):
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = 'test_discograph'
    POSTGRES_ROOT = '/usr/lib/postgresql/14'
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.PROCESS


class SqliteTestConfiguration(Configuration):
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.SQLITE
    SQLITE_DATABASE_NAME = os.path.join(tempfile.gettempdir(), 'discograph', 'test_discograph.db')
    APPLICATION_ROOT = 'http://localhost'
    THREADING_MODEL = ThreadingModel.THREAD


__all__ = [
    'DatabaseType',
    'ThreadingModel',
    'PostgresProductionConfiguration',
    'PostgresDevelopmentConfiguration',
    'PostgresTestConfiguration',
    'SqliteDevelopmentConfiguration',
    'SqliteTestConfiguration',
]
