#!/usr/bin/env python
import sys

from .config import get_config, configure_logging

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr-reduce config.py\n")
        sys.exit(1)

    config = get_config(sys.argv[1])

    configure_logging(config)

    try:
        for result in sys.stdin:
            config.REDUCE_FUNC(result.rstrip()) # remove trailing linebreak
    except (KeyboardInterrupt, SystemExit):
        config.OUTPUT_RESULTS_FUNC()
