import unittest

from discograph.config import (
    SqliteTestConfiguration,
    SqliteDevelopmentConfiguration,
    PostgresDevelopmentConfiguration,
)
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.logging_config import setup_logging, shutdown_logging


class TestCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_logging(is_testing=True)

    @classmethod
    def tearDownClass(cls):
        shutdown_logging()

    def test_01(self):
        setup_cache(vars(SqliteTestConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")

        assert cache is not None
        shutdown_cache()

    def test_02(self):
        cache_key = "test_key"
        setup_cache(vars(SqliteTestConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")
        assert cache is not None
        data = {
            "aaa": 111,
            "bbb": "222",
            "ccc": [333, 444],
        }
        cache.set(cache_key, data)
        actual = cache.get(cache_key)
        expected = data
        assert actual == expected

    def test_03(self):
        cache_key = "test_key"
        setup_cache(vars(SqliteTestConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")
        assert cache is not None
        data = {
            "aaa": 111,
            "bbb": "222",
            "ccc": [333, 444],
        }
        cache.set(cache_key, data)
        actual = cache.get(cache_key)
        expected = data
        assert actual == expected

        cache.set(cache_key, None)
        actual = cache.get(cache_key)
        expected = None
        assert actual == expected

    def test_04(self):
        setup_cache(vars(SqliteDevelopmentConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")

        assert cache is not None
        shutdown_cache()

    def test_05(self):
        cache_key = "test_key"
        setup_cache(vars(SqliteDevelopmentConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")
        assert cache is not None
        data = {
            "aaa": 111,
            "bbb": "222",
            "ccc": [333, 444],
        }
        cache.set(cache_key, data)
        actual = cache.get(cache_key)
        expected = data
        assert actual == expected

    def test_06(self):
        cache_key = "test_key"
        setup_cache(vars(SqliteDevelopmentConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")
        assert cache is not None
        data = {
            "aaa": 111,
            "bbb": "222",
            "ccc": [333, 444],
        }
        cache.set(cache_key, data)
        actual = cache.get(cache_key)
        expected = data
        assert actual == expected

        cache.set(cache_key, None)
        actual = cache.get(cache_key)
        expected = None
        assert actual == expected

    def test_postgres_01(self):
        setup_cache(vars(PostgresDevelopmentConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")

        assert cache is not None
        shutdown_cache()

    def test_postgres_02(self):
        cache_key = "test_key"
        setup_cache(vars(PostgresDevelopmentConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")
        assert cache is not None
        data = {
            "aaa": 111,
            "bbb": "222",
            "ccc": [333, 444],
        }
        cache.set(cache_key, data)
        actual = cache.get(cache_key)
        expected = data
        assert actual == expected

    def test_postgres_03(self):
        cache_key = "test_key"
        setup_cache(vars(PostgresDevelopmentConfiguration))

        from discograph.library.cache.cache_manager import cache

        print(f"test cache: {cache}")
        assert cache is not None
        data = {
            "aaa": 111,
            "bbb": "222",
            "ccc": [333, 444],
        }
        cache.set(cache_key, data)
        actual = cache.get(cache_key)
        expected = data
        assert actual == expected

        cache.set(cache_key, None)
        actual = cache.get(cache_key)
        expected = None
        assert actual == expected
