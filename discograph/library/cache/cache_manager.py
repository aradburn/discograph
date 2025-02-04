import logging
import os
import tempfile

from flask_caching import BaseCache, SimpleCache
from flask_caching.backends.rediscache import RedisCache
from flask_caching.backends.filesystemcache import FileSystemCache

from discograph.config import CacheType

log = logging.getLogger(__name__)

__all__ = [
    "CacheManager",
]


class CacheManager:
    cache: BaseCache | None = None

    @classmethod
    def setup_cache(cls, config) -> None:
        cls.cache = None

        # Based on configuration, use a different cache setup.
        match config["CACHE_TYPE"]:
            case CacheType.MEMORY:
                cls.cache = SimpleCache(threshold=1000000, default_timeout=0)
                log.info("Using memory cache")

            case CacheType.FILESYSTEM:
                file_cache_path = os.path.join(
                    tempfile.gettempdir(), "discograph", "cache"
                )
                file_cache_threshold = 1024 * 1024 * 20
                file_cache_timeout = 60 * 60 * 24 * 7
                if not os.path.exists(file_cache_path):
                    os.makedirs(file_cache_path)
                cls.cache = FileSystemCache(
                    file_cache_path,
                    default_timeout=file_cache_timeout,
                    threshold=file_cache_threshold,
                )
                log.info("Using filesystem cache")

            case CacheType.REDIS:
                cls.cache = RedisCache(
                    host="localhost",
                    port=6379,
                    password=None,
                    db=0,
                    default_timeout=60 * 60 * 24 * 7,
                    key_prefix=None,
                )
                # cls.cache = fakeredis.FakeRedis()
                if cls.cache is not None:
                    log.info("Using Redis cache")
                    print(f"cache: {cls.cache}")
                else:
                    cls.cache = SimpleCache(threshold=1000000, default_timeout=0)
                    log.info("No Redis found, falling back to using memory cache")

            case _:
                raise ValueError("Invalid CACHE_TYPE in configuration")

    @classmethod
    def shutdown_cache(cls) -> None:
        if cls.cache is not None:
            cls.cache.clear()
        cls.cache = None
        log.info("Shutdown cache")

    @classmethod
    def get_cache(cls) -> BaseCache:
        return cls.cache

    @classmethod
    def clear(cls) -> None:
        log.debug("Clearing cache")
        cls.cache.clear()
