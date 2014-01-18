#!/usr/bin/env python
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
            config.REDUCE_FUNC(result.rstrip()) # remove trailing linebreak
    except (KeyboardInterrupt, SystemExit):
        # we want to output results even if user aborted
        config.OUTPUT_RESULTS_FUNC()
    else:
        config.OUTPUT_RESULTS_FUNC()
