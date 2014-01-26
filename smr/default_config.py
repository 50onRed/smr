""" LOG_LEVEL: level of logging to be used for this job """
LOG_LEVEL = "info"
""" PARAMIKO_LOG_LEVEL: level of logging to be used for paramiko ssh connections (for smr-ec2 only) """
PARAMIKO_LOG_LEVEL = "warning"
"""
LOG_FORMAT:
  format of log messages
  available format params are:
    - message: actual log message
    - levelname: message log level
"""
LOG_FORMAT = "%(levelname)s:%(message)s"
"""
LOG_FILENAME:
  filename where log output for this job will be stored.
  available format params are:
    - config_name: basename of config file that's passed to smr
"""
LOG_FILENAME = "logs/%(config_name)s.log"

""" NUM_WORKERS: number of worker processes to use """
NUM_WORKERS = 4
""" DOWNLOAD_RETRIES: number of retries when downloading from S3 """
DOWNLOAD_RETRIES = 3
"""
OUTPUT_FILENAME:
  filename where results for this job will be stored.
  available format params are:
    - config_name: basename of config file that's passed to smr
    - time: current date and time
"""
OUTPUT_FILENAME = "results/%(config_name)s.%(time)s.out"

""" AWS_ACCESS_KEY: AWS access key used for S3 access """
AWS_ACCESS_KEY = None
""" AWS_SECRET_KEY: AWS secret key used for S3 access """
AWS_SECRET_KEY = None
"""
INPUT_DATA:
  List of files/directories that contain input data to be processed
  for example: ["s3://bucket/path"]
"""
INPUT_DATA = None

""" AWS_EC2_REGION: region to use when running ec2 workers """
AWS_EC2_REGION = "us-east-1"
""" AWS_EC2_AMI: ami to use when running ec2 workers """
AWS_EC2_AMI = "ami-8f311fe6"
""" AWS_EC2_INSTANCE_TYPE: instance type to use for EC2 instances """
AWS_EC2_INSTANCE_TYPE = "m1.large"
""" AWS_EC2_KEYNAME: keyname to use for starting EC2 instances """
AWS_EC2_KEYNAME = None
""" AWS_EC2_LOCAL_KEYFILE: local private key file used for ssh access to EC2 instances"""
AWS_EC2_LOCAL_KEYFILE = "~/.ssh/id_rsa"
""" AWS_EC2_SECURITY_GROUPS: security groups to use for accessing EC2 workers (needs port 22 open) """
AWS_EC2_SECURITY_GROUPS = ["default"]
""" AWS_EC2_SSH_USERNAME: username to use when logging into workers over SSH """
AWS_EC2_SSH_USERNAME = "ubuntu"
""" AWS_EC2_WORKERS: number of EC2 instances to use for this job """
AWS_EC2_WORKERS = 1

#from . import __version__
""" AWS_EC2_INITIALIZE_SMR_COMMANDS: commands to run for each EC2 instance to initialize smr """
#AWS_EC2_INITIALIZE_SMR_COMMANDS = ["while pgrep cloud-init > /dev/null; do sleep 1; done", "sudo apt-get update", "sudo apt-get -q -y install python-pip python-dev", "sudo pip install smr==%s" % __version__]
AWS_EC2_INITIALIZE_SMR_COMMANDS = ["while pgrep cloud-init > /dev/null; do sleep 1; done", "sudo apt-get update", "sudo apt-get -q -y install python-pip python-dev git", "sudo pip install git+git://github.com/idyedov/smr.git"]

""" AWS_EC2_REMOTE_CONFIG_PATH: where to store smr config on EC2 instances """
AWS_EC2_REMOTE_CONFIG_PATH = "/tmp/smr_config.py"

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
