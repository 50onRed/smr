from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import boto
from boto.s3.key import Key
from datetime import timedelta
import os
import re
import shutil
import sys

S3_BUCKETS = {} # cache s3 buckets to re-use them

def get_s3_bucket(bucket_name, config):
    if bucket_name not in S3_BUCKETS:
        if config.aws_access_key and config.aws_secret_key:
            s3conn = boto.connect_s3(config.aws_access_key, config.aws_secret_key)
        else:
            s3conn = boto.connect_s3() # use local boto config or IAM profile
        S3_BUCKETS[bucket_name] = s3conn.get_bucket(bucket_name)
    return S3_BUCKETS[bucket_name]

def date_generator(end_date, num_days):
    for n in reversed(xrange(num_days)):
        yield end_date - timedelta(n)

def get_s3_uri(m, file_names, config):
    """
    populates file_names list with urls that matched the regex match object
    returns the total filesize of all the files matched
    """
    bucket_name = m.group(1)
    path = m.group(2)
    bucket = get_s3_bucket(bucket_name, config)
    result = 0
    if (config.start_date or config.date_range) and ("{year" in path or "{month" in path or "{day" in path):
        for tmp_date in date_generator(config.end_date, config.date_range or ((config.end_date - config.start_date).days + 1)):
            # +1 because we want to include end_date
            for key in bucket.list(prefix=path.format(year=tmp_date.year, month=tmp_date.month, day=tmp_date.day)):
                file_names.append("s3://{}/{}".format(bucket_name, key.name))
                result += key.size
    else:
        for key in bucket.list(prefix=path):
            file_names.append("s3://{}/{}".format(bucket_name, key.name))
            result += key.size
    return result

def get_local_uri(m, file_names, _):
    """
    populates file_names list with urls that matched the regex match object
    returns the total filesize of all the files matched
    """
    path = m.group(2)
    result = 0
    for _, _, files in os.walk(path):
        for file_name in files:
            absolute_path = os.path.join(path, file_name)
            file_names.append("file:/{}".format(absolute_path))
            result += os.path.getsize(absolute_path)
    return result

def download_s3_uri(m, config):
    bucket_name = m.group(1)
    path = m.group(2)
    def download(local_file_name):
        bucket = get_s3_bucket(bucket_name, config)
        k = Key(bucket)
        k.key = path
        k.get_contents_to_filename(local_file_name)

    return download

def download_local_uri(m, _):
    path = m.group(2)
    def download(local_file_name):
        shutil.copyfile(path, local_file_name)

    return download

URI_REGEXES = [
    (re.compile(r"^s3://([^/]+)/?(.*)", re.IGNORECASE), get_s3_uri, download_s3_uri),
    (re.compile(r"^(file:/)?(/.*)", re.IGNORECASE), get_local_uri, download_local_uri)
]

def get_uris(config):
    """ returns a tuple of total file size in bytes, and the list of files """
    file_names = []
    if config.INPUT_DATA is None:
        sys.stderr.write("you need to provide INPUT_DATA in config\n")
        sys.exit(1)
    if isinstance(config.INPUT_DATA, basestring):
        config.INPUT_DATA = [config.INPUT_DATA]
    file_size = 0
    for uri in config.INPUT_DATA:
        for regex, uri_method, _ in URI_REGEXES:
            m = regex.match(uri)
            if m is not None:
                file_size += uri_method(m, file_names, config)
                break
    print("going to process {} files...".format(len(file_names)))
    return file_size, file_names

def get_download_method(config, uri):
    for regex, _, dl_method in URI_REGEXES:
        m = regex.match(uri)
        if m is not None:
            return dl_method(m, config)
