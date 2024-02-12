import logging
import os
import tempfile

from flask_caching import BaseCache, SimpleCache
from flask_caching.backends.filesystemcache import FileSystemCache
import fakeredis

from discograph.config import CacheType

log = logging.getLogger(__name__)

cache: BaseCache


def setup_cache(config):
    global cache
    # Based on configuration, use a different database.
    match config["CACHE_TYPE"]:

        case CacheType.MEMORY:
            cache = SimpleCache()
            log.info("Using memory cache")

        case CacheType.FILESYSTEM:
            file_cache_path = os.path.join(tempfile.gettempdir(), "discograph", "cache")
            file_cache_threshold = 1024 * 128
            file_cache_timeout = 60 * 60 * 24 * 7
            if not os.path.exists(file_cache_path):
                os.makedirs(file_cache_path)
            cache = FileSystemCache(
                file_cache_path,
                default_timeout=file_cache_timeout,
                threshold=file_cache_threshold,
            )
            log.info("Using filesystem cache")

        case CacheType.REDIS:
            cache = fakeredis.FakeRedis()
            # cache = RedisCache()
            log.info("Using Redis cache")

        case _:
            raise ValueError("Invalid CACHE_TYPE in configuration")


def shutdown_cache():
    global cache
    if cache is not None:
        cache.clear()
    cache = None
    log.info("Shutdown cache")

    # @classmethod
    # def cache_get(cls, key, use_redis=False):
    #     from discograph.app import app_redis_cache, app_file_cache
    #
    #     if use_redis:
    #         cache = app_redis_cache
    #     else:
    #         cache = app_file_cache
    #     data = cache.get(key)
    #     # log.debug('CACHE GET: {} [{}]'.format(data is not None, key))
    #     return data
    #
    # @staticmethod
    # def cache_clear(cls, use_redis=False):
    #     from discograph.app import app_redis_cache, app_file_cache
    #
    #     if use_redis:
    #         cache = app_redis_cache
    #     else:
    #         cache = app_file_cache
    #     cache.clear()
    #
    # @classmethod
    # def cache_set(cls, key, value, timeout=None, use_redis=False):
    #     from discograph.app import app_redis_cache, app_file_cache
    #
    #     if not timeout:
    #         timeout = 60 * 60 * 24
    #     if use_redis:
    #         cache = app_redis_cache
    #     else:
    #         cache = app_file_cache
    #     cache.set(key, value, timeout=timeout)
