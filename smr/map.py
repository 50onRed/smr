#!/usr/bin/env python
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import os
import sys
import tempfile

from .config import get_config, configure_job
from .uri import get_download_method

def write_to_stderr(prefix, file_name):
    sys.stderr.write("{0}{1}\n".format(prefix, file_name))
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
                config.MAP_FUNC(temp_filename)
                write_to_stderr("+", uri)
            except (KeyboardInterrupt, SystemExit):
                sys.stderr.write("map worker {0} aborted\n".format(os.getpid()))
                sys.exit(1)
            except Exception as e:
                sys.stderr.write("{0}\n".format(e))
                write_to_stderr("!", uri)
            finally:
                os.close(temp_file)
                try:
                    os.unlink(temp_filename)
                except OSError:
                    pass
    except (KeyboardInterrupt, SystemExit):
        sys.stderr.write("map worker {0} aborted\n".format(os.getpid()))
        sys.exit(1)

def main():
    config = get_config()
    run(config)
