"""
This module defines the main entry point for the Discograph data loader application.

It orchestrates the setup of various components, including logging, caching,
and database connections, and then initiates the data loading and transfer processes
using Luigi tasks.

The loader is responsible for:
    - Setting up logging for the application.
    - Initializing and managing the cache system.
    - Setting up connections to both the offline and runtime databases.
    - Defining and executing Luigi tasks for data loading and transfer.
    - Registering cleanup functions to be run when the application exits.
    - Running the loader process between specified dates.
"""

import atexit
import datetime
import logging
import sys

import luigi

from discograph.config import (
    PostgresDevelopmentConfiguration,
    SqliteDevelopmentConfiguration,
)
from discograph.library.cache.cache_manager import CacheManager
from discograph.logging_config import setup_logging
from discograph.offline.loader.loader_tasks import LoaderSetupTask
from discograph.offline.offline_database_manager import OfflineDatabaseManager
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager
from discograph.transfer.transfer_task import TransferTask

log = logging.getLogger(__name__)


def loader_main():
    """
    The main function for the Discograph data loader application.

    This function orchestrates the setup of the application environment, including:
        1. Configuring logging.
        2. Displaying application information in the log.
        3. Setting up the cache system.
        4. Establishing connections to the offline and runtime databases.
        5. Registering functions for graceful shutdown.
        6. Defining and executing the Luigi tasks for data loading and transfer.
    """
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

    # Run the loader process between these dates
    start_date = datetime.date(2024, 11, 1)
    # start_date = datetime.date(2023, 10, 1)
    end_date = datetime.date(2024, 11, 1)
    # end_date = datetime.datetime.now()
    tasks = [
        LoaderSetupTask(start_date=start_date, end_date=end_date),
        TransferTask(),
    ]
    luigi_run_result = luigi.build(
        tasks,
        detailed_summary=True,
        local_scheduler=True,
        log_level="WARNING",
    )
    print(luigi_run_result.summary_text)


if __name__ == "__main__":
    loader_main()
