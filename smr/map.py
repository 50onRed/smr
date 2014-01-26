#!/usr/bin/env python
from boto.s3.key import Key
import logging
import os
import sys
import tempfile

from .shared import get_config, get_s3_bucket, parse_s3_uri, configure_logging

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
            bucket_name, path = parse_s3_uri(uri)
            bucket = get_s3_bucket(bucket_name, config)
            k = Key(bucket)
            k.key = path
            temp_file, temp_filename = tempfile.mkstemp()
            tries = 0
            while True:
                try:
                    k.get_contents_to_filename(temp_filename)
                except (KeyboardInterrupt, SystemExit):
                    logging.error("map worker %d aborted", os.getpid())
                    sys.exit(1)
                except Exception as e:
                    logging.warn(e)
                    tries += 1
                    if tries >= config.DOWNLOAD_RETRIES:
                        logging.error("could not download file %s after %d tries", uri, tries)
                        write_to_stderr("!", uri)
                        continue # start processing the next file
                else:
                    break
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
