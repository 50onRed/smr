#!/usr/bin/env python
import boto
import boto.ec2
import datetime
import logging
import os
import paramiko
from Queue import Queue
import subprocess
import sys
import threading
import time

from .shared import get_config, reduce_thread, progress_thread, write_file_to_descriptor
from .uri import get_uris

def get_ssh_connection():
    ssh_connection = paramiko.SSHClient()
    ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    return ssh_connection

def worker_stdout_read_thread(output_queue, chan):
    stdout = chan.makefile("rb")
    for line in iter(stdout.readline, ""):
        output_queue.put(line)

def worker_stderr_read_thread(processed_files_queue, input_queue, chan, ssh, abort_event):

    stdin = chan.makefile("wb")
    stderr = chan.makefile_stderr("rb")

    # write first file to mapper
    if not abort_event.is_set() and not write_file_to_descriptor(input_queue, stdin):
        chan.shutdown_write()
        abort_event.set()
        sys.exit(1)

    for line in iter(stderr.readline, ""):
        line = line.rstrip() # remove trailing linebreak
        if line.startswith("+"):
            file_name = line[1:]
            logging.debug("successfully processed %s", file_name)
            processed_files_queue.put(file_name)
        elif line.startswith("!"):
            file_name = line[1:]
            logging.warn("error processing %s, requeuing...", file_name)
            input_queue.put(file_name) # re-queue file
        else:
            logging.error("invalid message received from mapper: %s", line)

        if abort_event.is_set():
            break
        if not write_file_to_descriptor(input_queue, stdin):
            # stdin.close() is not enough with paramiko to actually close it, need to do this too:
            chan.shutdown_write()
            break

        if chan.exit_status_ready():
            break

    while not chan.exit_status_ready():
        time.sleep(1)

    if not abort_event.is_set():
        exit_code = chan.recv_exit_status()
        if exit_code != 0:
            logging.error("map process exited with code %d", exit_code)

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

def initialize_instance(config, instance):
    ssh = get_ssh_connection()
    logging.info("waiting for ssh on instance %s %s ...", instance.id, instance.ip_address)
    while True:
        try:
            ssh.connect(instance.ip_address, username=config.aws_ec2_ssh_username, key_filename=os.path.expanduser(config.aws_ec2_local_keyfile))
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
        for line in iter(stdout.readline, ""):
            logging.debug(line.rstrip())
        for line in iter(stderr.readline, ""):
            logging.warn(line.rstrip())
        exit_code = chan.recv_exit_status()
        if exit_code != 0:
            logging.error("instance %s invalid exit code of %s: %d", instance.id, command, exit_code)
            ssh.close() # closes chan as well
            return False
        logging.info("instance %s successfully ran %s", instance.id, command)

    # copy config to this instance
    sftp = ssh.open_sftp()
    sftp.put(config.config, config.aws_ec2_remote_config_path)
    sftp.close()

    ssh.close()
    logging.info("instance %s successfully initialized", instance.id)
    return True

def main():
    config = get_config()
    print "logging to %s" % (config.log_filename)

    if not config.aws_ec2_keyname:
        sys.stderr.write("invalid AWS_EC2_KEYNAME\n")
        sys.exit(1)

    file_names = get_uris(config)
    files_total = len(file_names)

    input_queue = Queue(files_total)
    for file_name in file_names:
        input_queue.put(file_name)
    output_queue = Queue()
    processed_files_queue = Queue(files_total)

    start_time = datetime.datetime.now()

    conn = boto.ec2.connect_to_region(config.aws_ec2_region, aws_access_key_id=config.aws_access_key, aws_secret_access_key=config.aws_secret_key)
    reservation = conn.run_instances(image_id=config.aws_ec2_ami, min_count=config.aws_ec2_workers, max_count=config.aws_ec2_workers, \
                                     key_name=config.aws_ec2_keyname, instance_type=config.aws_ec2_instance_type, security_groups=config.aws_ec2_security_group)
    logging.info("requested to start %d instances", config.aws_ec2_workers)
    instances = reservation.instances
    instance_ids = [instance.id for instance in instances]
    for instance in instances:
        if not wait_for_instance(instance):
            sys.stderr.write("requested instances did not start, terminating all instances: %s\n" % ",".join(instance_ids))
            conn.terminate_instances(instance_ids)
            sys.exit(1)

        if not initialize_instance(config, instance):
            sys.stderr.write("could not initialize workers, terminating all instances: %s\n" % ",".join(instance_ids))
            conn.terminate_instances(instance_ids)
            sys.exit(1)

    abort_event = threading.Event()
    workers = []
    for instance in instances:
        for i in xrange(config.workers):

            ssh = get_ssh_connection()
            try:
                ssh.connect(instance.ip_address, username=config.aws_ec2_ssh_username, key_filename=os.path.expanduser(config.aws_ec2_local_keyfile))
            except:
                logging.error("could not ssh to %s %s", instance.id, instance.ip_address)
                abort_event.set()
                sys.exit(1)

            chan = ssh.get_transport().open_session()
            chan.exec_command("smr-map %s" % (config.aws_ec2_remote_config_path))

            stdout_thread = threading.Thread(target=worker_stdout_read_thread, args=(output_queue, chan))
            stdout_thread.daemon = True
            stdout_thread.start()

            stderr_thread = threading.Thread(target=worker_stderr_read_thread, args=(processed_files_queue, input_queue, chan, ssh, abort_event))
            stderr_thread.daemon = True
            stderr_thread.start()

            workers.append(stderr_thread)

    reduce_stdout = open(config.output_filename, "w")
    reduce_process = subprocess.Popen(["smr-reduce"] + sys.argv[1:], stdin=subprocess.PIPE, stdout=reduce_stdout)

    reduce_worker = threading.Thread(target=reduce_thread, args=(reduce_process, output_queue, abort_event))
    #reduce_worker.daemon = True
    reduce_worker.start()

    progress_worker = threading.Thread(target=progress_thread, args=(processed_files_queue, files_total, abort_event))
    #progress_worker.daemon = True
    progress_worker.start()

    try:
        for w in workers:
            w.join()
    except KeyboardInterrupt:
        abort_event.set()
        conn.terminate_instances(instance_ids)
        sys.stderr.write("\ruser aborted. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
        sys.stderr.write("partial results are in %s\n" % (config.output_filename))
        sys.exit(1)

    conn.terminate_instances(instance_ids)
    abort_event.set()
    # wait for reduce to finish before exiting
    reduce_worker.join()
    reduce_process.wait()
    reduce_stdout.close()
    sys.stderr.write("\rdone. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
    sys.stderr.write("results are in %s\n" % (config.output_filename))
