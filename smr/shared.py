import argparse
import curses
import datetime
import logging
import os
from Queue import Empty
import sys

from . import __version__

LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG
}
GLOBAL_SHARED_DATA = {
    "files_processed": 0,
    "last_file_processed": "",
    "messages": []
}

def ensure_dir_exists(path):
    dir_name = os.path.dirname(path)
    if dir_name != '' and not os.path.exists(dir_name):
        os.makedirs(dir_name)

def get_config(argparse_args=None):
    parser = get_arg_parser()
    args = parser.parse_args(argparse_args)

    config = get_config_module(args.config)

    # add extra options to args that cannot be specified in cli
    for arg in ("MAP_FUNC", "REDUCE_FUNC", "OUTPUT_RESULTS_FUNC", 
                "AWS_EC2_INITIALIZE_SMR_COMMANDS", "INPUT_DATA", "PIP_REQUIREMENTS"):
        setattr(args, arg, getattr(config, arg))
    args.args = argparse_args if argparse_args else sys.argv[1:]

    if not args.output_filename:
        args.output_filename = "results/{}.{}.out".format(args.config, datetime.datetime.now())
    ensure_dir_exists(args.output_filename)
    configure_logging(args)

    return args

class DummyConfig(object):
    MAP_FUNC = None
    REDUCE_FUNC = None
    OUTPUT_RESULTS_FUNC = None
    AWS_EC2_INITIALIZE_SMR_COMMANDS = None

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
        if not config_module.startswith("-"):
            sys.stderr.write("Invalid job definition provided: {0}\n".format(config_module))
            sys.exit(1)
        config = DummyConfig()

    # settings that are not overriden need to be set to defaults
    from . import default_config
    for k, v in default_config.__dict__.iteritems():
        if k.startswith("_"):
            continue
        if not hasattr(config, k):
            setattr(config, k, v)

    return config

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="config.py")

    parser.add_argument("--paramiko-log-level", help="level of logging to be used for paramiko ssh connections (for smr-ec2 only)", choices=LOG_LEVELS.keys(), default="warning")
    parser.add_argument("-w", "--workers", type=int, help="number of worker processes to use", default=8)
    parser.add_argument("--output-filename", help="filename where results for this job will be stored")
    parser.add_argument("--output-job-progress", help="Output job progress to screen", dest='output_job_progress', action='store_true', default=True)
    parser.add_argument("--no-output-job-progress", help="Do not output job progress to screen", dest='output_job_progress', action='store_false')
    parser.add_argument("--aws-access-key", help="AWS access key used for S3/EC2 access")
    parser.add_argument("--aws-secret-key", help="AWS secret key used for S3/EC2 access")
    parser.add_argument("--aws-ec2-region", help="region to use when running smr-ec2 workers", default="us-east-1")
    parser.add_argument("--aws-ec2-ami", help="AMI to use when running smr-ec2 workers", default="ami-89181be0")
    parser.add_argument("--aws-ec2-instance-type", help="instance type to use for EC2 instances", default="m3.large")
    parser.add_argument("--aws-ec2-keyname", help="keyname to use for starting EC2 instances")
    parser.add_argument("--aws-ec2-local-keyfile", help="local private key file used for ssh access to EC2 instances", default="~/.ssh/id_rsa")
    parser.add_argument("--aws-ec2-security-group", help="security group to use for accessing EC2 workers (needs port 22 open)", nargs="*", default=["default"])
    parser.add_argument("--aws-ec2-ssh-username", help="username to use when logging into EC2 workers over SSH", default="ubuntu")
    parser.add_argument("--aws-ec2-workers", help="number of EC2 instances to use for this job", type=int, default=1)
    parser.add_argument("--aws-ec2-remote-config-path", help="where to store smr config on EC2 instances", default="/tmp/smr_config.py")
    parser.add_argument("--cpu_usage_interval", type=float, help="interval used for measuring CPU usage in seconds", default=0.1)
    parser.add_argument("--screen_refresh_interval", type=float, help="how often to refresh job progress that's displayed on screen in seconds", default=1.0)

    parser.add_argument("--version", action="version", version="SMR {}".format(__version__))

    return parser

def configure_logging(config):
    paramiko_level_str = config.paramiko_log_level.lower()
    paramiko_level = LOG_LEVELS.get(paramiko_level_str, logging.WARNING)
    logging.getLogger("paramiko").setLevel(paramiko_level)

def reduce_thread(reduce_process, output_queue, abort_event):
    while not abort_event.is_set():
        try:
            # result has a trailing linebreak
            result = output_queue.get(timeout=2)
            if reduce_process.poll() is not None:
                # don't want to write if process has already terminated
                abort_event.set()
                break
            reduce_process.stdin.write(result)
            reduce_process.stdin.flush()
            output_queue.task_done()
        except Empty:
            pass
    reduce_process.stdin.close()

def print_pid(process, window, line_num, process_name):
    try:
        cpu_percent = process.cpu_percent(0.1)
    except:
        cpu_percent = 0.0
    add_str(window, line_num, "  {0} pid {1} CPU {2}".format(process_name, process.pid, cpu_percent))

def add_str(window, line_num, str):
    """ attempt to draw str on screen and ignore errors if they occur """
    try:
        window.addstr(line_num, 0, str)
    except curses.error:
        pass

def progress_thread(processed_files_queue, abort_event):
    while not abort_event.is_set():
        try:
            file_name = processed_files_queue.get(timeout=2)
            GLOBAL_SHARED_DATA["files_processed"] += 1
            GLOBAL_SHARED_DATA["last_file_processed"] = file_name
            processed_files_queue.task_done()
        except Empty:
            pass

def get_param(param):
    return GLOBAL_SHARED_DATA[param]

def add_message(message):
    GLOBAL_SHARED_DATA["messages"].append(message)

def write_file_to_descriptor(input_queue, descriptor):
    """
    get item from input_queue and write it to descriptor
    returns True if and only if it was successfully written
    """
    try:
        file_name = input_queue.get(timeout=2)
        descriptor.write("{0}\n".format(file_name))
        descriptor.flush()
        input_queue.task_done()
        return True
    except Empty:
        # no more files in queue
        descriptor.close()
        return False
    except IOError:
        return False # probably bad descriptor
