import atexit
import logging

from discograph.config import PostgresDevelopmentConfiguration
from discograph.database import setup_database, shutdown_database
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.logging_config import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


def updater_main():
    from discograph.library.database.database_helper import DatabaseHelper

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
    config = PostgresDevelopmentConfiguration()
    setup_cache(config)
    setup_database(config)
    # Note reverse order (last in first out), logging is the last to be shutdown
    atexit.register(shutdown_logging)
    atexit.register(shutdown_cache)
    atexit.register(shutdown_database, config)
    # Run the test update process
    DatabaseHelper.db_helper.update_tables("20230801")


if __name__ == "__main__":
    updater_main()