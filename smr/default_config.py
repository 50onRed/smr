LOG_LEVEL = "info"
PARAMIKO_LOG_LEVEL = "warning"
LOG_FORMAT = "%(levelname)s:%(message)s"
LOG_FILENAME = "logs/{config_name}.log"
NUM_WORKERS = 4
OUTPUT_FILENAME = "results/{config_name}.{time}.out"
AWS_ACCESS_KEY = None
AWS_SECRET_KEY = None
INPUT_DATA = None
CPU_REFRESH_INTERVAL = 0.1
SCREEN_REFRESH_INTERVAL = 1.0
AWS_EC2_REGION = "us-east-1"
AWS_EC2_AMI = "ami-89181be0"
AWS_EC2_INSTANCE_TYPE = "m3.large"
AWS_EC2_KEYNAME = None
AWS_EC2_LOCAL_KEYFILE = "~/.ssh/id_rsa"
AWS_EC2_SECURITY_GROUPS = ["default"]
AWS_EC2_SSH_USERNAME = "ubuntu"
AWS_EC2_WORKERS = 1
AWS_EC2_REMOTE_CONFIG_PATH = "/tmp/smr_config.py"

# commands to run for each EC2 instance to initialize smr
#from . import __version__
#AWS_EC2_INITIALIZE_SMR_COMMANDS = ["while pgrep cloud-init > /dev/null; do sleep 1; done", "sudo apt-get update", "sudo apt-get -q -y install python-pip python-dev", "sudo pip install smr=={0}".format(__version__)]
AWS_EC2_INITIALIZE_SMR_COMMANDS = ["while pgrep cloud-init > /dev/null; do sleep 1; done", "sudo apt-get update", "sudo apt-get -q -y install python-pip python-dev git", "sudo pip install git+git://github.com/idyedov/smr.git"]
PIP_REQUIREMENTS = None

# MAP_FUNC:
#   map function that will process input data
#   takes a single argument of local filename to be processed
MAP_FUNC = None

# REDUCE_FUNC:
#   reduce function that takes input from map function's STDOUT
#   takes a single string argument of a map function output
REDUCE_FUNC = None

def OUTPUT_RESULTS_FUNC():
    """ OUTPUT_RESULTS_FUNC: called upon job completion. takes no arguments """
    print "done."
