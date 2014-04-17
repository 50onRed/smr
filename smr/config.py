import argparse
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
        self.aws_ec2_region = "us-east-1"
        self.aws_ec2_ami = "ami-89181be0"
        self.aws_ec2_instance_type = "m3.large"
        self.aws_ec2_local_keyfile = "~/.ssh/id_rsa"
        self.aws_ec2_security_group = ["default"]
        self.aws_ec2_ssh_username = "ubuntu"
        self.aws_ec2_workers = 1
        self.aws_ec2_remote_config_path = "/tmp/smr_config.py"
        self.cpu_usage_interval = 0.1
        self.screen_refresh_interval = 1.0

def get_default_config():
    return DefaultConfig()

def ensure_dir_exists(path):
    dir_name = os.path.dirname(path)
    if dir_name != '' and not os.path.exists(dir_name):
        os.makedirs(dir_name)

def get_config_module(config_name):
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
        sys.stderr.write("Invalid job definition provided: {0}\n".format(config_module))
        sys.exit(1)

    # settings that are not overriden need to be set to defaults
    from . import default_config
    for k, v in default_config.__dict__.iteritems():
        if k.startswith("_"):
            continue
        if not hasattr(config, k):
            setattr(config, k, v)

    return config

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
    parser.add_argument("--aws-ec2-region", help="region to use when running smr-ec2 workers", default=default_config.aws_ec2_region)
    parser.add_argument("--aws-ec2-ami", help="AMI to use when running smr-ec2 workers", default=default_config.aws_ec2_ami)
    parser.add_argument("--aws-ec2-instance-type", help="instance type to use for EC2 instances", default=default_config.aws_ec2_instance_type)
    parser.add_argument("--aws-ec2-keyname", help="keyname to use for starting EC2 instances")
    parser.add_argument("--aws-ec2-local-keyfile", help="local private key file used for ssh access to EC2 instances", default=default_config.aws_ec2_local_keyfile)
    parser.add_argument("--aws-ec2-security-group", help="security group to use for accessing EC2 workers (needs port 22 open)", nargs="*", default=default_config.aws_ec2_security_group)
    parser.add_argument("--aws-ec2-ssh-username", help="username to use when logging into EC2 workers over SSH", default=default_config.aws_ec2_ssh_username)
    parser.add_argument("--aws-ec2-workers", help="number of EC2 instances to use for this job", type=int, default=default_config.aws_ec2_workers)
    parser.add_argument("--aws-ec2-remote-config-path", help="where to store smr config on EC2 instances", default=default_config.aws_ec2_remote_config_path)
    parser.add_argument("--cpu-usage-interval", type=float, help="interval used for measuring CPU usage in seconds", default=default_config.cpu_usage_interval)
    parser.add_argument("--screen-refresh-interval", type=float, help="how often to refresh job progress that's displayed on screen in seconds", default=default_config.screen_refresh_interval)

    parser.add_argument("-v", "--version", action="version", version="SMR {}".format(__version__))

    result = parser.parse_args(args)

    return result

def configure_job(args):
    config = get_config_module(args.config)

    # add extra options to args that cannot be specified in cli
    for arg in ("MAP_FUNC", "REDUCE_FUNC", "OUTPUT_RESULTS_FUNC",
                "AWS_EC2_INITIALIZE_SMR_COMMANDS", "INPUT_DATA", "PIP_REQUIREMENTS"):
        setattr(args, arg, getattr(config, arg))

    if not args.output_filename:
        args.output_filename = "results/{}.{}.out".format(args.config, datetime.datetime.now())

    # generate args to be passed to smr-map and smr-reduce
    args.args = []
    if args.aws_access_key:
        args.args.append("--aws-access-key")
        args.args.append(args.aws_access_key)
    if args.aws_secret_key:
        args.args.append("--aws-secret-key")
        args.args.append(args.aws_secret_key)
    args.args.append(args.config)

    ensure_dir_exists(args.output_filename)
    paramiko_level_str = args.paramiko_log_level.lower()
    paramiko_level = LOG_LEVELS.get(paramiko_level_str, logging.WARNING)
    logging.getLogger("paramiko").setLevel(paramiko_level)
