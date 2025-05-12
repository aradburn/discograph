import atexit
import logging
import sys

from discograph.config import (
    PostgresDevelopmentConfiguration,
    Configuration,
    TEXT_SEARCH_DATA,
    TEXT_SEARCH_FILENAME,
    DATA_DIR_KEY,
)
from discograph.library.cache.cache_manager import CacheManager
from discograph.logging_config import setup_logging
from discograph.offline.loader.loader_entity import LoaderEntity
from discograph.offline.offline_database_manager import OfflineDatabaseManager

log = logging.getLogger(__name__)


def create_search_index(_config: Configuration):
    setup_logging()
    log.info("")
    log.info("")
    log.info("######  #   # #   ####   ####   ####   ####    ##   #####  #    # ")
    log.info("#     # # #      #    # #    # #    # #    #  #  #  #    # #    # ")
    log.info("#     # #  ####  #      #    # #      #    # #    # #    # ###### ")
    log.info("#     # #      # #      #    # #  ### #####  ###### #####  #    # ")
    log.info("#     # # #    # #    # #    # #    # #   #  #    # #      #    # ")
    log.info("######  #  ####   ####   ####   ####  #    # #    # #      #    # ")
    log.info("")
    log.info("")
    log.info("Using PostgresDevelopmentConfiguration")
    # log.info(f"DATABASE_HOST: {os.getenv('DISCOGRAPH_DATABASE_HOST')}")
    # log.info(f"DATABASE_NAME: {os.getenv('DISCOGRAPH_DATABASE_NAME')}")

    # Setup Cache
    CacheManager.setup_cache(_config)
    cache = CacheManager.get_cache()
    print(f"cache: {cache}")
    if cache is None:
        log.error("Cache not set")
        sys.exit()
    else:
        log.debug("Clearing cache")
        CacheManager.clear()

    OfflineDatabaseManager.setup_database(_config)

    # Note reverse order (last in first out), logging is the last to be shutdown
    # atexit.register(shutdown_logging)
    atexit.register(CacheManager.shutdown_cache)
    atexit.register(OfflineDatabaseManager.shutdown_database)

    text_search_path = _config[DATA_DIR_KEY] / TEXT_SEARCH_DATA / TEXT_SEARCH_FILENAME
    LoaderEntity().loader_create_text_search_index(text_search_path)


if __name__ == "__main__":
    _config = PostgresDevelopmentConfiguration()
    create_search_index(_config)
