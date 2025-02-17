import unittest

from discograph.config import (
    SqliteOfflineTestConfiguration,
    DatabaseType,
    PostgresOfflineTestConfiguration,
    PostgresProductionConfiguration,
    PostgresRuntimeTestConfiguration,
    SqliteRuntimeTestConfiguration,
)


class TestConfiguration(unittest.TestCase):
    def test_SqliteOfflineTestConfiguration(self):
        config = SqliteOfflineTestConfiguration()
        self.assertIsNotNone(config)

    def test_SqliteOfflineTestConfiguration_get_testing(self):
        config = SqliteOfflineTestConfiguration()
        self.assertTrue(config["TESTING"])

    def test_SqliteOfflineTestConfiguration_get_database(self):
        config = SqliteOfflineTestConfiguration()
        self.assertEqual(DatabaseType.SQLITE, config["DATABASE"])

    def test_SqliteOfflineTestConfiguration_set(self):
        config = SqliteOfflineTestConfiguration()
        with self.assertRaises(TypeError) as ctx:
            # noinspection PyUnresolvedReferences
            config["TESTING"] = False
        self.assertEqual(
            "'SqliteOfflineTestConfiguration' object does not support item assignment",
            str(ctx.exception),
        )

    def test_SqliteRuntimeTestConfiguration(self):
        config = SqliteRuntimeTestConfiguration()
        self.assertIsNotNone(config)

    def test_SqliteRuntimeTestConfiguration_get_testing(self):
        config = SqliteRuntimeTestConfiguration()
        self.assertTrue(config["TESTING"])

    def test_SqliteRuntimeTestConfiguration_get_database(self):
        config = SqliteRuntimeTestConfiguration()
        self.assertEqual(DatabaseType.SQLITE, config["DATABASE"])

    def test_SqliteRuntimeTestConfiguration_set(self):
        config = SqliteRuntimeTestConfiguration()
        with self.assertRaises(TypeError) as ctx:
            # noinspection PyUnresolvedReferences
            config["TESTING"] = False
        self.assertEqual(
            "'SqliteRuntimeTestConfiguration' object does not support item assignment",
            str(ctx.exception),
        )

    def test_PostgresOfflineTestConfiguration(self):
        config = PostgresOfflineTestConfiguration()
        self.assertIsNotNone(config)

    def test_PostgresOfflineTestConfiguration_get_testing(self):
        config = PostgresOfflineTestConfiguration()
        self.assertTrue(config["TESTING"])

    def test_PostgresOfflineTestConfiguration_get_database(self):
        config = PostgresOfflineTestConfiguration()
        self.assertEqual(DatabaseType.POSTGRES, config["DATABASE"])

    def test_PostgresOfflineTestConfiguration_set(self):
        config = PostgresOfflineTestConfiguration()
        with self.assertRaises(TypeError) as ctx:
            # noinspection PyUnresolvedReferences
            config["TESTING"] = False
        self.assertEqual(
            "'PostgresOfflineTestConfiguration' object does not support item assignment",
            str(ctx.exception),
        )

    def test_PostgresRuntimeTestConfiguration(self):
        config = PostgresRuntimeTestConfiguration()
        self.assertIsNotNone(config)

    def test_PostgresRuntimeTestConfiguration_get_testing(self):
        config = PostgresRuntimeTestConfiguration()
        self.assertTrue(config["TESTING"])

    def test_PostgresRuntimeTestConfiguration_get_database(self):
        config = PostgresRuntimeTestConfiguration()
        self.assertEqual(DatabaseType.POSTGRES, config["DATABASE"])

    def test_PostgresRuntimeTestConfiguration_set(self):
        config = PostgresRuntimeTestConfiguration()
        with self.assertRaises(TypeError) as ctx:
            # noinspection PyUnresolvedReferences
            config["TESTING"] = False
        self.assertEqual(
            "'PostgresRuntimeTestConfiguration' object does not support item assignment",
            str(ctx.exception),
        )

    def test_PostgresProductionConfiguration(self):
        config = PostgresProductionConfiguration()
        self.assertIsNotNone(config)

    def test_PostgresProductionConfiguration_get_testing(self):
        config = PostgresProductionConfiguration()
        self.assertFalse(config["TESTING"])

    def test_PostgresProductionConfiguration_get_database(self):
        config = PostgresProductionConfiguration()
        self.assertEqual(DatabaseType.POSTGRES, config["DATABASE"])

    def test_PostgresProductionConfiguration_set(self):
        config = PostgresProductionConfiguration()
        with self.assertRaises(TypeError) as ctx:
            # noinspection PyUnresolvedReferences
            config["TESTING"] = False
        self.assertEqual(
            "'PostgresProductionConfiguration' object does not support item assignment",
            str(ctx.exception),
        )
