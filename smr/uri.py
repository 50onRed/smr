import boto
from boto.s3.key import Key
import logging
import os
import re
import shutil
import sys

S3_BUCKETS = {} # cache s3 buckets to re-use them

def get_s3_bucket(bucket_name, config):
    if bucket_name not in S3_BUCKETS:
        s3conn = boto.connect_s3(config.aws_access_key, config.aws_secret_key)
        S3_BUCKETS[bucket_name] = s3conn.get_bucket(bucket_name)
    return S3_BUCKETS[bucket_name]

def get_s3_uri(m, file_names, config):
    bucket_name = m.group(1)
    path = m.group(2)
    bucket = get_s3_bucket(bucket_name, config)
    for key in bucket.list(prefix=path):
        file_names.append("s3://%s/%s" % (bucket_name, key.name))

def get_local_uri(m, file_names, config):
    path = m.group(2)
    for root, sub_folders, files in os.walk(path):
        for file in files:
            file_names.append("file:/%s" % (os.path.join(path, file)))

def download_s3_uri(m, config):
    bucket_name = m.group(1)
    path = m.group(2)
    def download(local_file_name):
        bucket = get_s3_bucket(bucket_name, config)
        k = Key(bucket)
        k.key = path
        k.get_contents_to_filename(local_file_name)

    return download

def download_local_uri(m, config):
    path = m.group(2)
    def download(local_file_name):
        shutil.copyfile(path, local_file_name)

    return download

URI_REGEXES = [
    (re.compile(r"^s3://([^/]+)/?(.*)", re.IGNORECASE), get_s3_uri, download_s3_uri),
    (re.compile(r"^(file:/)?(/.*)", re.IGNORECASE), get_local_uri, download_local_uri)
]

def get_uris(config):
    file_names = []
    if config.input_data is None:
        logging.error("you need to provide INPUT_DATA in config or as an argument")
        sys.exit(1)
    if isinstance(config.input_data, basestring):
        config.input_data = [config.input_data]
    for uri in config.input_data:
        for regex, uri_method, dl_method in URI_REGEXES:
            m = regex.match(uri)
            if m is not None:
                uri_method(m, file_names, config)
                break
    logging.info("going to process %d files...", len(file_names))
    return file_names

def get_download_method(config, uri):
    for regex, uri_method, dl_method in URI_REGEXES:
        m = regex.match(uri)
        if m is not None:
            return dl_method(m, config)
