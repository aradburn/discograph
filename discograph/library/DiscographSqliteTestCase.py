# -*- encoding: utf-8 -*-
import unittest
from functools import wraps

from peewee import SqliteDatabase
from discograph.library.Bootstrapper import Bootstrapper
from discograph.library.SqliteEntity import SqliteEntity
from discograph.library.SqliteMaster import SqliteMaster
from discograph.library.SqliteModel import SqliteModel
from discograph.library.SqliteRelation import SqliteRelation
from discograph.library.SqliteRelease import SqliteRelease

TEST_DATABASE = '/home/andy/test.db'
# TEST_DATABASE = ':memory:'

# Create a database instance that will manage the connection and
# execute queries
test_database = SqliteDatabase(TEST_DATABASE, pragmas={
    'journal_mode': 'off',
    'synchronous': 0,
    'cache_size': 1000000,
    # 'locking_mode': 'exclusive',
    'temp_store': 'memory',
})

test_models = (
    SqliteEntity,
    SqliteMaster,
    SqliteModel,
    SqliteRelation,
    SqliteRelease,
)


# Bind the given models to the db for the duration of wrapped block.
def use_test_database(fn):
    @wraps(fn)
    def inner(self):
        with test_database.bind_ctx(test_models):
            test_database.create_tables(test_models)
            try:
                fn(self)
            finally:
                test_database.drop_tables(test_models)
    return inner


class DiscographSqliteTestCase(unittest.TestCase):

    # test_database = pool.PooledPostgresqlExtDatabase(
    #     'test_discograph',
    #     host='127.0.0.1',
    #     user=app.config['POSTGRESQL_USERNAME'],
    #     password=app.config['POSTGRESQL_PASSWORD'],
    #     )

    @classmethod
    def setUpTestDB(cls):
        print("setupTestDB")
        Bootstrapper.is_test = True
        with test_database.bind_ctx(test_models):
            print(SqliteModel._meta.database.database)
            SqliteModel.bootstrap_sqlite_models(pessimistic=True)

    def run(self, result=None):
        import discograph
        self.setUpTestDB()
        discograph.Bootstrapper.is_test = True
        with test_database.bind_ctx(test_models):
            super(DiscographSqliteTestCase, self).run(result)
