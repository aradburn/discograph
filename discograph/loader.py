import atexit
import datetime
import logging

import luigi

from discograph.config import PostgresDevelopmentConfiguration
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.library.loader.loader_tasks import LoaderSetupTask
from discograph.logging_config import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


def loader_main():
    from discograph.database import setup_database, shutdown_database

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

    # Run the loader process between these dates
    start_date = datetime.date(2024, 6, 1)
    # start_date = datetime.date(2023, 10, 1)
    end_date = datetime.datetime.now()
    tasks = [LoaderSetupTask(start_date=start_date, end_date=end_date)]
    luigi_run_result = luigi.build(
        tasks,
        detailed_summary=True,
        local_scheduler=True,
        log_level="WARNING",
    )
    print(luigi_run_result.summary_text)


if __name__ == "__main__":
    loader_main()
