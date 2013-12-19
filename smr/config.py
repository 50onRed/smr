import logging
import os
import sys

LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG
}

def get_config(config_name):
    if config_name.endswith(".py"):
        config_name = config_name[:-3]
    elif config_name.endswith(".pyc"):
        config_name = config_name[:-4]

    directory, config_module = os.path.split(config_name)

    # If the directory isn't in the PYTHONPATH, add it so our import will work
    if directory not in sys.path:
        sys.path.insert(0, directory)

    config = __import__(config_module)

    return config

def configure_logging(config):
    level_str = config.LOG_LEVEL.lower()
    level = LOG_LEVELS.get(level_str, logging.INFO)
    logging.basicConfig(level=level)

    if level_str not in LOG_LEVELS:
        logging.warn("invalid value for LOG_LEVEL: %s", config.LOG_LEVEL)
