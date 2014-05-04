#!/usr/bin/env python
"""
Usage: `smr common_crawl_domains.py` or `smr-ec2 common_crawl_domains.py`

This sample job computes the number of times each domain appears in the dataset and outputs that in descending order.
Uses commoncrawl dataset from http://commoncrawl.org/ that's hosted on S3: http://aws.amazon.com/datasets/41740
"""
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import gzip
import json
import sys
from urlparse import urlparse
try:
    import warc
except ImportError:
    print("you need to install warc python extension to run this job")
    sys.exit(1)

# use only a small chunk of it for testing purposes
INPUT_DATA = "s3://aws-publicdatasets/common-crawl/crawl-002/2010/09/25/45"
global_result = {}

# These are required to run smr-ec2, 
# they can be passed on the command line as:
# --aws-access-key, --aws-secret-key

PIP_REQUIREMENTS = ["warc==0.2.1"]

def MAP_FUNC(file_name):
    result = {}

    with gzip.open(file_name) as f:
        w = warc.ARCFile(fileobj=f)
        for record in w:
            domain = urlparse(record.header.url).hostname
            result[domain] = result.get(domain, 0) + 1

    print(json.dumps(result))

def REDUCE_FUNC(result):
    j = json.loads(result)
    for key, count in j.iteritems():
        global_result[key] = global_result.get(key, 0) + count

def OUTPUT_RESULTS_FUNC():
    for key, count in sorted(global_result.iteritems(), key=lambda x: x[1], reverse=True):
        print("{},{}".format(key, count))
