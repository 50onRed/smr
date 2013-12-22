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

def worker_thread(config, config_name, input_queue, output_queue, processed_files_queue, abort_event):

    # start ec2 instance
    conn = boto.ec2.connect_to_region(config.AWS_EC2_REGION, aws_access_key_id=config.AWS_ACCESS_KEY, aws_secret_access_key=config.AWS_SECRET_KEY)
    reservation = conn.run_instances(config.AWS_EC2_AMI, key_name=config.AWS_EC2_KEYNAME, instance_type=config.AWS_EC2_INSTANCE_TYPE, security_groups=config.AWS_EC2_SECURITY_GROUPS)
    instance = reservation.instances[0]
    status = instance.update()
    while status == "pending":
        sys.stderr.write(".")
        sys.stderr.flush()
        time.sleep(2)
        status = instance.update()
    sys.stderr.write("\n")

    # wait for ec2 instance to start
    if status != "running":
        logging.error("Could not start instance: %s", status)
        abort_event.set()
        return

    logging.info("New instance %s started: %s", instance.id, instance.ip_address)

    # wait for ssh to start up on ec2 instance
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
        ssh.exec_command(command)

    while not abort_event.is_set():
        try:
            file_name = input_queue.get(timeout=2)
            stdin, stdout, stderr = ssh.exec_command("smr-map %s" % config_name)
            stdin.write("%s" % file_name)
            stdin.close()
            for line in stdout:
                output_queue.put(line)
            processed_files_queue.put(file_name)
            logging.debug("instance %d processed %s", instance.id, file_name)
            input_queue.task_done()
        except Empty:
            pass
    ssh.close()

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
    abort_event = threading.Event()

    workers = []
    for i in xrange(config.NUM_WORKERS):
        w = threading.Thread(target=worker_thread, args=(config, config_name, input_queue, output_queue, processed_files_queue, abort_event))
        #w.daemon = True
        w.start()
        workers.append(w)

    reduce_stdout = None
    if config.OUTPUT_FILENAME is not None:
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
        sys.stderr.write("\ruser aborted. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
        sys.stderr.write("partial results are in %s\n" % ("STDOUT" if config.OUTPUT_FILENAME is None else config.OUTPUT_FILENAME))
        sys.exit(1)

    abort_event.set()
    if reduce_stdout is not None:
        reduce_stdout.close()
    sys.stderr.write("\rdone. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
    sys.stderr.write("results are in %s\n" % ("STDOUT" if config.OUTPUT_FILENAME is None else config.OUTPUT_FILENAME))
