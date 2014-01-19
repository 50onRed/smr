#!/usr/bin/env python
import logging
import sys

from .shared import get_config, configure_logging

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr-reduce config.py\n")
        sys.exit(1)

    config = get_config(sys.argv[1])
    configure_logging(config)

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
