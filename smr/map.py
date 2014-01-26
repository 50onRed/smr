#!/usr/bin/env python
import logging
import os
import sys
import tempfile

from .shared import get_config, configure_logging
from .uri import get_download_method

def write_to_stderr(prefix, file_name):
    sys.stderr.write("%s%s\n" % (prefix, file_name))
    sys.stderr.flush()

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr-map config.py\n")
        sys.exit(1)

    config = get_config(sys.argv[1])
    configure_logging(config)

    try:
        logging.debug("mapper starting to read stdin")
        for uri in iter(sys.stdin.readline, ""):
            uri = uri.rstrip() # remove trailing linebreak
            logging.debug("mapper got %s", uri)
            temp_file, temp_filename = tempfile.mkstemp()
            dl = get_download_method(config, uri)
            dl(temp_filename)
            try:
                config.MAP_FUNC(temp_filename)
                write_to_stderr("+", uri)
            except (KeyboardInterrupt, SystemExit):
                logging.error("map worker %d aborted", os.getpid())
                sys.exit(1)
            except Exception as e:
                logging.error(e)
                write_to_stderr("!", uri)
            finally:
                os.close(temp_file)
                os.unlink(temp_filename)
    except (KeyboardInterrupt, SystemExit):
        logging.error("map worker %d aborted", os.getpid())
        sys.exit(1)
