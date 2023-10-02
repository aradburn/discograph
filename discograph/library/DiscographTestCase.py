# -*- encoding: utf-8 -*-
import unittest
from functools import wraps

from peewee import SqliteDatabase
from discograph.library.Bootstrapper import Bootstrapper
from discograph.library.PostgresEntity import PostgresEntity
from discograph.library.PostgresMaster import PostgresMaster
from discograph.library.PostgresModel import PostgresModel
from discograph.library.PostgresRelation import PostgresRelation
from discograph.library.PostgresRelease import PostgresRelease

TEST_DATABASE = 'test.db'
# TEST_DATABASE = ':memory:'

# Create a database instance that will manage the connection and
# execute queries
test_database = SqliteDatabase(TEST_DATABASE)

test_models = (
    PostgresEntity,
    PostgresMaster,
    PostgresModel,
    PostgresRelation,
    PostgresRelease,
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


class DiscographTestCase(unittest.TestCase):

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
            print(PostgresModel._meta.database.database)
            PostgresModel.bootstrap_postgres_models(pessimistic=True)

    def run(self, result=None):
        import discograph
        print("\nrun")
        self.setUpTestDB()
        discograph.Bootstrapper.is_test = True
        with test_database.bind_ctx(test_models):
            super(DiscographTestCase, self).run(result)
