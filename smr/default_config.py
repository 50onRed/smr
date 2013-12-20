""" LOG_LEVEL: level of logging to be used for this job """
LOG_LEVEL = "info"
"""
LOG_FORMAT:
  format of log messages
  available format params are:
    - message: actual log message
    - levelname: message log level
"""
LOG_FORMAT = "%(levelname)s:%(message)s"

""" NUM_WORKERS: number of worker processes to use """
NUM_WORKERS = 4
""" DOWNLOAD_RETRIES: number of retries when downloading from S3 """
DOWNLOAD_RETRIES = 3
"""
OUTPUT_FILENAME:
  filename where results for this job will be stored.
  results will be output to STDOUT if set to None
  available format params are:
    - config_name: basename of config file that's passed to smr
    - time: current date and time
"""
OUTPUT_FILENAME = "results/%(config_name)s.%(time)s.out"

""" AWS_ACCESS_KEY: AWS access key used for S3 access """
AWS_ACCESS_KEY = None
""" AWS_SECRET_KEY: AWS secret key used for S3 access """
AWS_SECRET_KEY = None
""" S3_BUCKET_NAME: S3 bucket name that contains files that we want to process """
S3_BUCKET_NAME = None
""" S3_FILE_PREFIXES: list of S3 file prefixes that we're interested in for this job """
S3_FILE_PREFIXES = [""]

"""
MAP_FUNC:
  map function that will process s3 data
  takes a single argument of local filename to be processed
"""
MAP_FUNC = None
"""
REDUCE_FUNC:
  reduce function that takes input from map function's STDOUT
  takes a single string argument of a map function output
"""
REDUCE_FUNC = None

def OUTPUT_RESULTS_FUNC():
    """ OUTPUT_RESULTS_FUNC: called upon job completion. takes no arguments """
    print "done."
