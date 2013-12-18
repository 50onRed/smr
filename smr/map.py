#!/usr/bin/env python
import boto
from boto.s3.key import Key
import logging
import os
import sys
import tempfile

from .config import get_config

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr-map config.py\n")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO)

    config = get_config(sys.argv[1])

    s3conn = boto.connect_s3(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    bucket = s3conn.get_bucket(config.S3_BUCKET_NAME)
    for file_name in sys.stdin:
        file_name = file_name.rstrip() # remove trailing linebreak
        k = Key(bucket)
        k.key = file_name
        temp_file, temp_filename = tempfile.mkstemp()
        k.get_contents_to_filename(temp_filename)
        try:
            config.MAP_FUNC(temp_filename)
        finally:
            os.close(temp_file)
            os.unlink(temp_filename)
