#!/usr/bin/env python
"""
Usage: `smr common_crawl.py` or `smr-ec2 common-crawl.py`

This sample job computes the number of times each domain appears in the dataset and outputs that in descending order.
Uses commoncrawl dataset from http://commoncrawl.org/ that's hosted on S3: http://aws.amazon.com/datasets/41740
"""
import gzip
import json
import sys
from urlparse import urlparse
try:
    import warc
except ImportError:
    print "you need to install warc python extension to run this job"
    sys.exit(1)

# use only a small chunk of it for testing purposes
INPUT_DATA = "s3://aws-publicdatasets/common-crawl/crawl-002/2010/09/25/45"
global_result = {}

# These are required to run smr-ec2, 
# they can also be passed on the command line, 
# as: --aws-access-key, --aws-secret-key, --aws-ec2-keyname, --aws-ec2-local-keyfile
# AWS_ACCESS_KEY = ''
# AWS_SECRET_KEY = ''
# AWS_EC2_KEYNAME = ''
# AWS_EC2_LOCAL_KEYFILE = ''

def MAP_FUNC(file_name):
    result = {}

    with gzip.open(file_name) as f:
        w = warc.ARCFile(fileobj=f)
        for record in w:
            domain = urlparse(record.header.url).hostname
            result[domain] = result.get(domain, 0) + 1

    print json.dumps(result)

def REDUCE_FUNC(result):
    j = json.loads(result)
    for key, count in j.iteritems():
        global_result[key] = global_result.get(key, 0) + count

def OUTPUT_RESULTS_FUNC():
    for key, count in sorted(global_result.iteritems(), key=lambda x : x[1], reverse=True):
        print "%s,%d" % (key.encode("utf-8"), count)
