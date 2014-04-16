#!/usr/bin/env python
import sys

from .shared import get_config_from_cmd_args

def run(config):
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
    config = get_config_from_cmd_args()
    run(config)
