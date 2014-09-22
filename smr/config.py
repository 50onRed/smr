from __future__ import absolute_import, division, print_function, unicode_literals
import argparse
import boto
import datetime
import logging
import os
import sys

from .version import __version__

LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG
}

class DefaultConfig(object):
    def __init__(self):
        self.paramiko_log_level = "warning"
        self.workers = 8
        self.output_job_progress = True
        self.aws_access_key = None
        self.aws_secret_key = None
        self.aws_ec2_region = "us-east-1"
        self.aws_ec2_ami = "ami-30837058"
        self.aws_ec2_instance_type = "m3.large"
        self.aws_ec2_security_group = ["default"]
        self.aws_ec2_ssh_username = "ubuntu"
        self.aws_ec2_workers = 1
        self.aws_ec2_remote_config_path = "/tmp/smr_config.py"
        self.aws_ec2_initialization_commands = [
            "sudo apt-get update",
            #"sudo apt-get -q -y install python-pip python-dev",
            "sudo apt-get -q -y install python-pip python-dev git",
            #"sudo pip install smr=={}".format(__version__)",
            "sudo pip install git+git://github.com/idyedov/smr.git"
        ]
        self.aws_iam_profile = None
        self.cpu_usage_interval = 0.1
        self.screen_refresh_interval = 1.0
        self.date_range = None
        self.start_date = None
        self.end_date = None

def get_default_config():
    return DefaultConfig()

def get_config_module(config_name):
    if not os.path.isfile(config_name):
        sys.stderr.write("job definition does not exist: {}\n".format(config_name))
        sys.exit(1)

    if config_name.endswith(".py"):
        config_name = config_name[:-3]
    elif config_name.endswith(".pyc"):
        config_name = config_name[:-4]

    directory, config_module = os.path.split(config_name)

    # If the directory isn't in the PYTHONPATH, add it so our import will work
    if directory not in sys.path:
        sys.path.insert(0, directory)

    try:
        config = __import__(config_module)
    except ImportError:
        sys.stderr.write("Could not import job definition: {}\n".format(config_module))
        sys.exit(1)

    if not hasattr(config, "MAP_FUNC"):
        setattr(config, "MAP_FUNC", None)
    if not hasattr(config, "REDUCE_FUNC"):
        setattr(config, "REDUCE_FUNC", None)
    if not hasattr(config, "OUTPUT_RESULTS_FUNC"):
        def default_output_results_func():
            print("done")
        setattr(config, "OUTPUT_RESULTS_FUNC", default_output_results_func)

    return config

def mkdate(datestring):
    return datetime.datetime.strptime(datestring, "%Y-%m-%d").date()

def get_config(args=None):
    default_config = DefaultConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="config.py")

    parser.add_argument("--paramiko-log-level", help="level of logging to be used for paramiko ssh connections (for smr-ec2 only)", choices=LOG_LEVELS.keys(), default=default_config.paramiko_log_level)
    parser.add_argument("-w", "--workers", type=int, help="number of worker processes to use", default=default_config.workers)
    parser.add_argument("--output-filename", help="filename where results for this job will be stored")
    parser.add_argument("--output-job-progress", help="Output job progress to screen", dest='output_job_progress', action='store_true', default=default_config.output_job_progress)
    parser.add_argument("--no-output-job-progress", help="Do not output job progress to screen", dest='output_job_progress', action='store_false')
    parser.add_argument("--aws-access-key", help="AWS access key used for S3/EC2 access")
    parser.add_argument("--aws-secret-key", help="AWS secret key used for S3/EC2 access")
    parser.add_argument("--aws-iam-profile", help="AWS IAM profile to use when launching EC2 instances")
    parser.add_argument("--aws-ec2-region", help="region to use when running smr-ec2 workers", default=default_config.aws_ec2_region)
    parser.add_argument("--aws-ec2-ami", help="AMI to use when running smr-ec2 workers", default=default_config.aws_ec2_ami)
    parser.add_argument("--aws-ec2-instance-type", help="instance type to use for EC2 instances", default=default_config.aws_ec2_instance_type)
    parser.add_argument("--aws-ec2-security-group", help="security group to use for accessing EC2 workers (needs port 22 open)", nargs="*", default=default_config.aws_ec2_security_group)
    parser.add_argument("--aws-ec2-ssh-username", help="username to use when logging into EC2 workers over SSH", default=default_config.aws_ec2_ssh_username)
    parser.add_argument("--aws-ec2-workers", help="number of EC2 instances to use for this job", type=int, default=default_config.aws_ec2_workers)
    parser.add_argument("--aws-ec2-remote-config-path", help="where to store smr config on EC2 instances", default=default_config.aws_ec2_remote_config_path)
    parser.add_argument("--aws-ec2-initialization-commands", help="initialization commands to use for EC2 instances", nargs="+", default=default_config.aws_ec2_initialization_commands)
    parser.add_argument("--cpu-usage-interval", type=float, help="interval used for measuring CPU usage in seconds", default=default_config.cpu_usage_interval)
    parser.add_argument("--screen-refresh-interval", type=float, help="how often to refresh job progress that's displayed on screen in seconds", default=default_config.screen_refresh_interval)
    parser.add_argument("--start-date", type=mkdate, help="start date (YYYY-mm-dd) for this job, only used if using {year}/{month}/{day} macros in INPUT_DATA")
    parser.add_argument("--end-date", type=mkdate, help="end date (YYYY-mm-dd) for this job, only used if using {year}/{month}/{day} macros in INPUT_DATA", default=datetime.datetime.utcnow().date())
    parser.add_argument("--date-range", type=int, help="number of days back to process, overrides start date if used")

    parser.add_argument("-v", "--version", action="version", version="SMR {}".format(__version__))

    result = parser.parse_args(args)

    return result

def configure_job(args):
    config = get_config_module(args.config)

    # add extra options to args that cannot be specified in cli
    for arg in ("MAP_FUNC", "REDUCE_FUNC", "OUTPUT_RESULTS_FUNC", "INPUT_DATA"):
        setattr(args, arg, getattr(config, arg))

    pip_requirements = getattr(config, "PIP_REQUIREMENTS", None)
    if pip_requirements:
        for package in pip_requirements:
            args.aws_ec2_initialization_commands.append("sudo pip install {}".format(package))

    # if we don't have aws credentials and no iam profile in config, attempt to use iam profile of current instance
    if not args.aws_iam_profile and (not args.aws_access_key or not args.aws_secret_key):
        metadata = boto.utils.get_instance_metadata(timeout=1.0, num_retries=1, data='meta-data/iam/security-credentials/')
        if len(metadata) > 0:
            args.aws_iam_profile = metadata.keys()[0]

    paramiko_level_str = args.paramiko_log_level.lower()
    paramiko_level = LOG_LEVELS.get(paramiko_level_str, logging.WARNING)
    logging.getLogger("paramiko").setLevel(paramiko_level)
    logging.getLogger("paramiko.transport").addHandler(logging.NullHandler())
