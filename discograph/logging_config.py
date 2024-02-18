import logging.config

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "error": {
            "format": "%(asctime)s-%(levelname)s-%(name)s-%(process)d::%(module)s|%(lineno)s:: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "console_handler": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        # "rotating_file_handler": {
        #     "level": "INFO",
        #     "formatter": "standard",
        #     "class": "logging.handlers.RotatingFileHandler",
        #     "filename": LOGGING_FILE,
        #     "mode": "a",
        #     "maxBytes": 1048576,
        #     "backupCount": 10,
        # },
        # "error_file_handler": {
        #     "level": "WARNING",
        #     "formatter": "error",
        #     "class": "logging.FileHandler",
        #     "filename": LOGGING_ERROR_FILE,
        #     "mode": "a",
        # },
        # "critical_mail_handler": {
        #     "level": "CRITICAL",
        #     "formatter": "error",
        #     "class": "logging.handlers.SMTPHandler",
        #     "mailhost": "localhost",
        #     "fromaddr": "monitoring@discograph.com",
        #     "toaddrs": ["dev@discograph.com", "qa@discograph.com"],
        #     "subject": "Critical error with Discograph application",
        # },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "WARNING",
            "propagate": False,
        },
        "discograph": {
            "handlers": ["console_handler"],
            "level": "DEBUG",
            "propagate": False,
        },
        "__main__": {  # if __name__ == '__main__'
            "handlers": ["console_handler"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}

TEST_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "error": {
            "format": "%(asctime)s-%(levelname)s-%(name)s-%(process)d::%(module)s|%(lineno)s:: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "console_handler": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        # "debug_file_handler": {
        #     "level": "DEBUG",
        #     "formatter": "standard",
        #     "class": "logging.FileHandler",
        #     "filename": LOGGING_DEBUG_FILE,
        #     # "mode": "a",
        # },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "WARNING",
            "propagate": False,
        },
        "discograph": {
            "handlers": ["console_handler"],
            "level": "DEBUG",
            "propagate": False,
        },
        "tests": {
            "handlers": ["console_handler"],
            "level": "DEBUG",
            "propagate": False,
        },
        "__main__": {  # if __name__ == '__main__'
            "handlers": ["console_handler"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}


def setup_logging(is_testing=False):
    # Run once at startup:
    config = LOGGING_CONFIG if is_testing is False else TEST_LOGGING_CONFIG
    logging.config.dictConfig(config)

    # Include this next line in each module that needs logging:
    log = logging.getLogger(__name__)
    log.info("")
    log.info("Logging configured OK.")


def shutdown_logging():
    log = logging.getLogger(__name__)
    log.info("Shutting down logging.")
    logging.shutdown()
