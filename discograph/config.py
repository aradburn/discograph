import enum
import os


class DatabaseType(enum.Enum):

    POSTGRES = 1
    SQLITE = 2


class Configuration(object):
    DEBUG = False
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_ROOT = '/usr/lib/postgresql/14/'
    POSTGRES_DATABASE_NAME = 'discograph'
    APPLICATION_ROOT = 'http://discograph.mbrsi.org'
    FILE_CACHE_PATH = os.path.join(os.path.dirname(__file__), '..', 'tmp')
    FILE_CACHE_THRESHOLD = 1024 * 128
    FILE_CACHE_TIMEOUT = 60 * 60 * 24 * 7


class PostgresDevelopmentConfiguration(Configuration):
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = 'discograph'
    POSTGRES_ROOT = '/usr/lib/postgresql/14'


class SqliteDevelopmentConfiguration(Configuration):
    DEBUG = True
    TESTING = False
    DATABASE = DatabaseType.SQLITE
    SQLITE_DATABASE_NAME = '/tmp/discograph.db'


class PostgresTestConfiguration(Configuration):
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.POSTGRES
    POSTGRES_DATABASE_NAME = 'test_discograph'
    POSTGRES_ROOT = '/usr/lib/postgresql/14'


class SqliteTestConfiguration(Configuration):
    DEBUG = True
    TESTING = True
    DATABASE = DatabaseType.SQLITE
    SQLITE_DATABASE_NAME = '/tmp/test_discograph.db'


__all__ = [
    'DatabaseType',
    'Configuration',
    'PostgresDevelopmentConfiguration',
    'PostgresTestConfiguration',
    'SqliteDevelopmentConfiguration',
    'SqliteTestConfiguration',
    ]
