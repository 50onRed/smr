#!/usr/bin/env python
import logging
import sys

from .shared import get_config

def main():
    config = get_config()

    try:
        for result in iter(sys.stdin.readline, ""):
            result = result.rstrip() # remove trailing linebreak
            logging.debug("smr-reduce got %s", result)
            config.REDUCE_FUNC(result)
    except (KeyboardInterrupt, SystemExit):
        # we want to output results even if user aborted
        config.OUTPUT_RESULTS_FUNC()
    else:
        config.OUTPUT_RESULTS_FUNC()
