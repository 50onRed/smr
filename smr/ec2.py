#!/usr/bin/env python
import boto
import boto.ec2
import datetime
import logging
import multiprocessing
import os
import paramiko
from Queue import Empty
import subprocess
import sys
import threading
import time

from .shared import get_config, configure_logging, get_files_to_process, reduce_thread, progress_thread

def get_ssh_connection():
    ssh_connection = paramiko.SSHClient()
    ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    return ssh_connection

def worker_thread(config, config_name, input_queue, output_queue, processed_files_queue, abort_event, instance):
    ssh = get_ssh_connection()
    try:
        ssh.connect(instance.ip_address, username=config.AWS_EC2_SSH_USERNAME, key_filename=os.path.expanduser(config.AWS_EC2_LOCAL_KEYFILE))
    except:
        logging.error("could not ssh to %s %s", instance.id, instance.ip_address)
        abort_event.set()
        return

    while not abort_event.is_set():
        try:
            file_name = input_queue.get(timeout=2)
            chan = ssh.get_transport().open_session()
            chan.exec_command("echo '%s' | smr-map %s" % (file_name, config.AWS_EC2_REMOTE_CONFIG_PATH))
            stdout = chan.makefile("rb")
            exit_code = chan.recv_exit_status()
            if exit_code != 0:
                logging.error("instance %s map process exited with code %d", instance.id, exit_code)
                input_queue.put(file_name) # requeue file
                input_queue.task_done()
                continue
            for line in stdout:
                output_queue.put(line)
            processed_files_queue.put(file_name)
            logging.debug("instance %d processed %s", instance.id, file_name)
            input_queue.task_done()
        except Empty:
            pass
    ssh.close()

def wait_for_instance(instance):
    """ wait for instance status to be 'running' in which case return True, False otherwise """

    status = instance.update()
    logging.info("waiting for instance %s ...", instance.id)

    while status == "pending":
        sys.stderr.write(".")
        sys.stderr.flush()
        time.sleep(2)
        status = instance.update()

    sys.stderr.write("\n")

    if status != "running":
        logging.error("Invalid status when starting instance %s: %s", instance.id, status)
        return False

    logging.info("New instance %s started: %s", instance.id, instance.ip_address)
    return True

def initialize_instance(config, config_name, instance):
    ssh = get_ssh_connection()
    logging.info("waiting for ssh on instance %s %s ...", instance.id, instance.ip_address)
    while True:
        try:
            ssh.connect(instance.ip_address, username=config.AWS_EC2_SSH_USERNAME, key_filename=os.path.expanduser(config.AWS_EC2_LOCAL_KEYFILE))
        except:
            sys.stderr.write(".")
            sys.stderr.flush()
            time.sleep(2)
            continue
        else:
            break
    sys.stderr.write("\n")

    # initialize smr on this ec2 instance
    for command in config.AWS_EC2_INITIALIZE_SMR_COMMANDS:
        chan = ssh.get_transport().open_session()
        chan.exec_command(command)
        stdout = chan.makefile("rb")
        stderr = chan.makefile_stderr("rb")
        exit_code = chan.recv_exit_status()
        for line in stdout:
            logging.debug(line.rstrip())
        for line in stderr:
            logging.warn(line.rstrip())
        if exit_code != 0:
            logging.error("instance %s invalid exit code of %s: %d", instance.id, command, exit_code)
            ssh.close() # closes chan as well
            return False
        logging.info("instance %s successfully ran %s", instance.id, command)

    # copy config to this instance
    sftp = ssh.open_sftp()
    sftp.put(config_name, config.AWS_EC2_REMOTE_CONFIG_PATH)
    sftp.close()

    ssh.close()
    logging.info("instance %s successfully initialized", instance.id)
    return True

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr-ec2 config.py\n")
        sys.exit(1)

    config_name = sys.argv[1]
    config = get_config(config_name)

    configure_logging(config)

    file_names = get_files_to_process(config)
    files_total = len(file_names)

    input_queue = multiprocessing.JoinableQueue(files_total)
    for file_name in file_names:
        input_queue.put(file_name)
    output_queue = multiprocessing.Queue()
    processed_files_queue = multiprocessing.Queue()

    start_time = datetime.datetime.now()

    conn = boto.ec2.connect_to_region(config.AWS_EC2_REGION, aws_access_key_id=config.AWS_ACCESS_KEY, aws_secret_access_key=config.AWS_SECRET_KEY)
    logging.info("requested to start %d instances", config.AWS_EC2_WORKERS)
    reservation = conn.run_instances(image_id=config.AWS_EC2_AMI, min_count=config.AWS_EC2_WORKERS, max_count=config.AWS_EC2_WORKERS, \
                                     key_name=config.AWS_EC2_KEYNAME, instance_type=config.AWS_EC2_INSTANCE_TYPE, security_groups=config.AWS_EC2_SECURITY_GROUPS)
    instances = reservation.instances
    instance_ids = [instance.id for instance in instances]
    for instance in instances:
        if not wait_for_instance(instance):
            sys.stderr.write("requested instances did not start, terminating all instances: %s\n" % ",".join(instance_ids))
            conn.terminate_instances(instance_ids)
            sys.exit(1)

        if not initialize_instance(config, config_name, instance):
            sys.stderr.write("could not initialize workers, terminating all instances: %s\n" % ",".join(instance_ids))
            conn.terminate_instances(instance_ids)
            sys.exit(1)

    abort_event = threading.Event()
    workers = []
    for instance in instances:
        for i in xrange(config.NUM_WORKERS):
            w = threading.Thread(target=worker_thread, args=(config, config_name, input_queue, output_queue, processed_files_queue, abort_event, instance))
            #w.daemon = True
            w.start()
            workers.append(w)

    reduce_stdout = open(config.OUTPUT_FILENAME, "w")
    reduce_process = subprocess.Popen(["smr-reduce", config_name], stdin=subprocess.PIPE, stdout=reduce_stdout)

    reduce_worker = threading.Thread(target=reduce_thread, args=(reduce_process, output_queue, abort_event))
    #reduce_worker.daemon = True
    reduce_worker.start()

    progress_worker = threading.Thread(target=progress_thread, args=(processed_files_queue, files_total, abort_event))
    #progress_worker.daemon = True
    progress_worker.start()

    try:
        input_queue.join()
    except KeyboardInterrupt:
        abort_event.set()
        conn.terminate_instances(instance_ids)
        sys.stderr.write("\ruser aborted. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
        sys.stderr.write("partial results are in %s\n" % (config.OUTPUT_FILENAME))
        sys.exit(1)

    conn.terminate_instances(instance_ids)
    abort_event.set()
    reduce_stdout.close()
    sys.stderr.write("\rdone. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
    sys.stderr.write("results are in %s\n" % (config.OUTPUT_FILENAME))
