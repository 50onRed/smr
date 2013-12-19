LOG_LEVEL = "info"
LOG_FORMAT = "%(levelname)s:%(message)s"

NUM_WORKERS = 4

AWS_ACCESS_KEY = None
AWS_SECRET_KEY = None
S3_BUCKET_NAME = None
S3_FILE_PREFIXES = []

MAP_FUNC = None
REDUCE_FUNC = None

def OUTPUT_RESULTS_FUNC():
    print "done."
