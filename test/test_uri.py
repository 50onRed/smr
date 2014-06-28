from smr import get_default_config
from smr.uri import get_uris

import sure
from moto import mock_s3
import boto
from boto.s3.key import Key

def upload_file(bucket, file):
    k = Key(bucket)
    k.key = file
    k.set_contents_from_string(file)

def get_config_for_prefix(prefix):
    config = get_default_config()
    config.aws_access_key = "test_access_key"
    config.aws_secret_key = "test_secret_key"
    config.INPUT_DATA = prefix
    return config

@mock_s3
def test_get_uris():
    conn = boto.connect_s3()
    bucket = conn.create_bucket('mybucket')
    upload_file(bucket, "dir1/file1.csv")
    upload_file(bucket, "dir1/dir2/file2.csv")
    upload_file(bucket, "file3.csv")

    config = get_config_for_prefix("s3://mybucket")
    bytes_total, uris = get_uris(config)
    len(uris).should.equal(3)
    uris.should.have("s3://mybucket/dir1/file1.csv")
    uris.should.have("s3://mybucket/dir1/dir2/file2.csv")
    uris.should.have("s3://mybucket/file3.csv")

    config = get_config_for_prefix("s3://mybucket/dir1")
    bytes_total, uris = get_uris(config)
    len(uris).should.equal(2)
    uris.should.have("s3://mybucket/dir1/file1.csv")
    uris.should.have("s3://mybucket/dir1/dir2/file2.csv")
