#!/usr/bin/env python
import sys

from .shared import get_config

def main():
    config = get_config()

    try:
        for result in iter(sys.stdin.readline, ""):
            result = result.rstrip() # remove trailing linebreak
            config.REDUCE_FUNC(result)
    finally:
        # we want to output results even if user aborted
        config.OUTPUT_RESULTS_FUNC()
