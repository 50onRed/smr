#!/usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals
from inspect import getargspec
import os
import sys

from .config import get_config, configure_job
from .uri import download, cleanup

def write_to_stderr(file_status, file_size, file_name):
    sys.stderr.write("{},{},{}\n".format(file_status, file_size, file_name))
    sys.stderr.flush()

def run(config):
    configure_job(config)
    try:
        for uri in iter(sys.stdin.readline, ""):
            uri = uri.rstrip() # remove trailing linebreak
            temp_filename = None
            try:
                temp_filename = download(config, uri)
                file_size = os.path.getsize(temp_filename)
                # allow passing uri to mapper, without breaking existing code
                if len(getargspec(config.MAP_FUNC).args) == 2:
                    config.MAP_FUNC(temp_filename, uri)
                else:
                    config.MAP_FUNC(temp_filename)
                write_to_stderr("+", file_size, uri)
            except (KeyboardInterrupt, SystemExit):
                sys.stderr.write("map worker {} aborted\n".format(os.getpid()))
                sys.exit(1)
            except Exception as e:
                sys.stderr.write("{}\n".format(e))
                write_to_stderr("!", 0, uri)
            finally:
                sys.stdout.flush() # force stdout flush after every file processed
                if temp_filename:
                    cleanup(uri, temp_filename)
    except (KeyboardInterrupt, SystemExit):
        sys.stderr.write("map worker {} aborted\n".format(os.getpid()))
        sys.exit(1)

def main():
    config = get_config()
    run(config)
