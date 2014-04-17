#!/usr/bin/env python
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import sys

from .config import get_config, configure_job

def run(config):
    configure_job(config)
    try:
        for result in iter(sys.stdin.readline, ""):
            result = result.rstrip() # remove trailing linebreak
            config.REDUCE_FUNC(result)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        # we want to output results even if user aborted
        config.OUTPUT_RESULTS_FUNC()

def main():
    config = get_config()
    run(config)
