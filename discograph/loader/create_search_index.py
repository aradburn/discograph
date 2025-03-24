import atexit
import logging
import sys

from discograph.config import (
    PostgresDevelopmentConfiguration,
    SqliteDevelopmentConfiguration, TEXT_SEARCH_PATH,
)
from discograph.library.cache.cache_manager import CacheManager
from discograph.logging_config import setup_logging
from discograph.offline.loader.loader_entity import LoaderEntity
from discograph.offline.offline_database_manager import OfflineDatabaseManager
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

log = logging.getLogger(__name__)


def create_search_index():
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
    offline_config = PostgresDevelopmentConfiguration()
    runtime_config = SqliteDevelopmentConfiguration()

    # Setup Cache
    CacheManager.setup_cache(offline_config)
    cache = CacheManager.get_cache()
    print(f"cache: {cache}")
    if cache is None:
        log.error("Cache not set")
        sys.exit()
    else:
        log.debug("Clearing cache")
        CacheManager.clear()

    OfflineDatabaseManager.setup_database(offline_config)
    RuntimeDatabaseManager.setup_database(runtime_config)

    # Note reverse order (last in first out), logging is the last to be shutdown
    # atexit.register(shutdown_logging)
    atexit.register(CacheManager.shutdown_cache)
    atexit.register(OfflineDatabaseManager.shutdown_database)
    atexit.register(RuntimeDatabaseManager.shutdown_database)

    LoaderEntity().loader_create_text_search_index(TEXT_SEARCH_PATH)


if __name__ == "__main__":
    create_search_index()
