import datetime
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

    # settings that are not overriden need to be set to defaults
    from . import default_config
    for k, v in default_config.__dict__.iteritems():
        if k.startswith("_"):
            continue
        if not hasattr(config, k):
            setattr(config, k, v)

    if config.OUTPUT_FILENAME is not None:
        config.OUTPUT_FILENAME = config.OUTPUT_FILENAME % {"config_name": config_module, "time": datetime.datetime.now()}

    return config

def configure_logging(config):
    level_str = config.LOG_LEVEL.lower()
    level = LOG_LEVELS.get(level_str, logging.INFO)
    logging.basicConfig(level=level, format=config.LOG_FORMAT)

    if level_str not in LOG_LEVELS:
        logging.warn("invalid value for LOG_LEVEL: %s", config.LOG_LEVEL)
