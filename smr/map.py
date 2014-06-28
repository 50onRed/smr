#!/usr/bin/env python
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import os
import sys
import tempfile

from .config import get_config, configure_job
from .uri import get_download_method

def write_to_stderr(file_status, file_size, file_name):
    sys.stderr.write("{},{},{}\n".format(file_status, file_size, file_name))
    sys.stderr.flush()

def run(config):
    configure_job(config)
    try:
        for uri in iter(sys.stdin.readline, ""):
            uri = uri.rstrip() # remove trailing linebreak
            temp_file, temp_filename = tempfile.mkstemp()
            dl = get_download_method(config, uri)
            try:
                dl(temp_filename)
                file_size = os.path.getsize(temp_filename)
                config.MAP_FUNC(temp_filename)
                write_to_stderr("+", file_size, uri)
            except (KeyboardInterrupt, SystemExit):
                sys.stderr.write("map worker {} aborted\n".format(os.getpid()))
                sys.exit(1)
            except Exception as e:
                sys.stderr.write("{}\n".format(e))
                write_to_stderr("!", 0, uri)
            finally:
                os.close(temp_file)
                try:
                    os.unlink(temp_filename)
                except OSError:
                    pass
    except (KeyboardInterrupt, SystemExit):
        sys.stderr.write("map worker {} aborted\n".format(os.getpid()))
        sys.exit(1)

def main():
    config = get_config()
    run(config)
