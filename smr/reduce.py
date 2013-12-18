#!/usr/bin/env python
import logging
import sys

from .config import get_config

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr-reduce config.py\n")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO)

    config = get_config(sys.argv[1])

    try:
        for result in sys.stdin:
            config.REDUCE_FUNC(result)
    except (KeyboardInterrupt, SystemExit):
        config.OUTPUT_RESULTS_FUNC()
