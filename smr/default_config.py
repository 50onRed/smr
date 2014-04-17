from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

# commands to run for each EC2 instance to initialize smr
from .version import __version__
AWS_EC2_INITIALIZE_SMR_COMMANDS = [
    "while pgrep cloud-init > /dev/null; do sleep 1; done",
    "DEBIAN_FRONTEND=noninteractive",
    "sudo apt-get update",
    "sudo apt-get -q -y install python-pip python-dev",
    #"sudo apt-get -q -y install python-pip python-dev git",
    "sudo pip install smr=={}".format(__version__)
    #"sudo pip install git+git://github.com/idyedov/smr.git"
]
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
    print("done.")
