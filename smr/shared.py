import boto
import datetime
import logging
import os
from Queue import Empty
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

    paramiko_level_str = config.PARAMIKO_LOG_LEVEL.lower()
    paramiko_level = LOG_LEVELS.get(paramiko_level_str, logging.WARNING)
    logging.getLogger("paramiko").setLevel(paramiko_level)

def get_files_to_process(config):
    file_names = []
    logging.info("getting list of files from s3...")
    s3conn = boto.connect_s3(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    bucket = s3conn.get_bucket(config.S3_BUCKET_NAME)
    for prefix in config.S3_FILE_PREFIXES:
        for key in bucket.list(prefix=prefix):
            file_names.append(key.name)
    logging.info("going to process %d files...", len(file_names))
    return file_names

def reduce_thread(reduce_process, output_queue, abort_event):
    while True:
        if abort_event.is_set():
            reduce_process.terminate()
            break
        try:
            result = output_queue.get(timeout=2)
            if reduce_process.poll() is not None:
                # don't want to write if process has already terminated
                logging.error("reduce process %d ended with code %d", reduce_process.pid, reduce_process.returncode)
                abort_event.set()
                break
            reduce_process.stdin.write(result)
        except Empty:
            pass

def progress_thread(processed_files_queue, files_total, abort_event):
    files_processed = 0
    while not abort_event.is_set():
        try:
            file_name = processed_files_queue.get(timeout=2)
            logging.debug("master received signal that %s is processed", file_name)
            files_processed += 1
            sys.stderr.write("\rprocessed {0:%}".format(files_processed / float(files_total)))
        except Empty:
            pass