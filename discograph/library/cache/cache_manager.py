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
    """
    Manages the application's cache system.

    This class provides a centralized way to configure, access, and clear the cache.
    It supports different cache types such as memory, filesystem, and Redis.

    Attributes:
        cache (BaseCache | None): The active cache instance. It can be None if the cache is not yet initialized.
    """

    cache: BaseCache | None = None

    @classmethod
    def setup_cache(cls, config) -> None:
        """
        Initializes the cache based on the provided configuration.

        The cache type is determined by the 'CACHE_TYPE' key in the config dictionary.
        Supported cache types are:
            - CacheType.MEMORY: Uses a SimpleCache for in-memory caching.
            - CacheType.FILESYSTEM: Uses a FileSystemCache for file-based caching.
            - CacheType.REDIS: Uses a RedisCache for caching in a Redis server.

        Args:
            config (dict): A dictionary containing the application's configuration.
                           The 'CACHE_TYPE' key is used to determine the cache type.

        Raises:
            ValueError: If an invalid 'CACHE_TYPE' is provided in the configuration.
        """
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
        """
        Clears and shuts down the cache.

        This method clears the cache and sets the cache attribute to None,
        releasing any resources held by the cache.
        """
        if cls.cache is not None:
            cls.cache.clear()
        cls.cache = None
        log.info("Shutdown cache")

    @classmethod
    def get_cache(cls) -> BaseCache:
        """
        Returns the current cache instance.

        Returns:
            BaseCache: The current cache instance.
        """
        return cls.cache

    @classmethod
    def clear(cls) -> None:
        """
        Clears all data from the cache.
        """
        log.debug("Clearing cache")
        cls.cache.clear()
