#!/usr/bin/env python
"""
Usage: `smr common_crawl_words.py` or `smr-ec2 common_crawl_words.py`

This sample job computes the number of times each domain appears in the dataset and outputs that in descending order.
Uses commoncrawl dataset from http://commoncrawl.org/ that's hosted on S3: http://aws.amazon.com/datasets/41740
"""
import gzip
import re
import sys
try:
    from bs4 import BeautifulSoup
except ImportError:
    print "you need to install beautifulsoup4 python extension to run this job"
    sys.exit(1)
try:
    import warc
except ImportError:
    print "you need to install warc python extension to run this job"
    sys.exit(1)

# use only a small chunk of it for testing purposes
INPUT_DATA = "s3://aws-publicdatasets/common-crawl/crawl-002/2010/09/25/45"
global_result = {}

# These are required to run smr-ec2, 
# they can be passed on the command line as:
# --aws-access-key, --aws-secret-key, --aws-ec2-keyname, --aws-ec2-local-keyfile

PIP_REQUIREMENTS = ["beautifulsoup4==4.3.2", "warc==0.2.1"]

REGEX_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z0-9 ]")
REGEX_SPACE = re.compile(r"\s+")
REGEX_DOUBLE_LINEBREAK = re.compile(r"\r\n\r\n")

def MAP_FUNC(file_name):
    with gzip.open(file_name) as f:
        w = warc.ARCFile(fileobj=f)
        for record in w:
            if "text" not in record.header.content_type:
                continue
            splt = REGEX_DOUBLE_LINEBREAK.split(record.payload, 1)
            if len(splt) < 2:
                continue
            soup = BeautifulSoup(splt[1])
            payload = REGEX_NON_ALPHANUMERIC.sub("", soup.get_text(u" ", strip=True))
            for word in REGEX_SPACE.split(payload):
                print word # pass word to reducer

def REDUCE_FUNC(word):
    word = word.rstrip() # remove trailing linebreak
    global_result[word] = global_result.get(word, 0) + 1

def OUTPUT_RESULTS_FUNC():
    for word, count in sorted(global_result.iteritems(), key=lambda x: x[1], reverse=True):
        print "{0},{1}".format(word.encode("utf-8"), count)
