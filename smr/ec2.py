#!/usr/bin/env python
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import boto
import boto.ec2
import curses
import datetime
import os
import paramiko
import psutil
from Queue import Queue
import subprocess
import sys
import threading
import time

from .version import __version__
from .config import get_config, configure_job
from .shared import reduce_thread, progress_thread, write_file_to_descriptor, print_pid, get_param, add_message, add_str
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
            processed_files_queue.put(file_name)
        elif line.startswith("!"):
            file_name = line[1:]
            add_message("error processing {0}, requeuing...".format(file_name))
            input_queue.put(file_name)
        else:
            add_message("invalid message received from mapper: {0}".format(line))

        if abort_event.is_set():
            break
        if not write_file_to_descriptor(input_queue, stdin):
            # stdin.close() is not enough with paramiko to actually close it, need to do this too:
            chan.shutdown_write()
            break

        if chan.exit_status_ready():
            break

    if not abort_event.is_set():
        while not chan.exit_status_ready():
            time.sleep(1)

    ssh.close()

def wait_for_instance(instance):
    """ wait for instance status to be 'running' in which case return True, False otherwise """

    status = None
    print("getting status for instance {0} ...".format(instance.id))
    while status is None:
        status = instance.update()
        if status is None:
            time.sleep(2)
    print("waiting for instance {0} ...".format(instance.id))

    while status == "pending":
        time.sleep(2)
        status = instance.update()

    if status != "running":
        print("Invalid status when starting instance {0}: {1}".format(instance.id, status))
        return False

    print("New instance {0} started: {1}".format(instance.id, instance.ip_address))
    return True

def initialize_instance_thread(config, instance, abort_event):
    if not wait_for_instance(instance):
        abort_event.set()
        return

    ssh = get_ssh_connection()
    print("waiting for ssh on instance {0} {1} ...".format(instance.id, instance.ip_address))
    while True:
        try:
            ssh.connect(instance.ip_address, username=config.aws_ec2_ssh_username, key_filename=os.path.expanduser(config.aws_ec2_local_keyfile))
        except:
            time.sleep(2)
            continue
        else:
            break

    # initialize smr on this ec2 instance
    for command in config.AWS_EC2_INITIALIZE_SMR_COMMANDS:
        run_command(ssh, instance, command)

    if config.PIP_REQUIREMENTS is not None and len(config.PIP_REQUIREMENTS) > 0:
        for package in config.PIP_REQUIREMENTS:
            run_command(ssh, instance, "sudo pip install {0}".format(package))

    # copy config to this instance
    sftp = ssh.open_sftp()
    sftp.put(config.config, config.aws_ec2_remote_config_path)
    sftp.close()

    ssh.close()
    print("instance {0} successfully initialized".format(instance.id))

def run_command(ssh, instance, command):
    chan = ssh.get_transport().open_session()
    chan.exec_command(command)
    #stdout = chan.makefile("rb")
    stderr = chan.makefile_stderr("rb")
    #for line in iter(stdout.readline, ""):
    #    print(line.rstrip())
    for line in iter(stderr.readline, ""):
        print("instance {0} stderr: {1}".format(instance.id, line.rstrip()))
    exit_code = chan.recv_exit_status()
    if exit_code != 0:
        print("instance {0} invalid exit code of {1}: {2}".format(instance.id, command, exit_code))
        ssh.close() # closes chan as well
        return False
    print("instance {0} successfully ran {1}".format(instance.id, command))

def start_worker(config, instance, abort_event, output_queue, processed_files_queue, input_queue):
    ssh = get_ssh_connection()

    try:
        ssh.connect(instance.ip_address, username=config.aws_ec2_ssh_username, key_filename=os.path.expanduser(config.aws_ec2_local_keyfile))
    except:
        print("could not ssh to {0} {1}".format(instance.id, instance.ip_address))
        abort_event.set()
        sys.exit(1)

    chan = ssh.get_transport().open_session()
    chan.exec_command("smr-map {0}".format(" ".join(config.args[:-1] + [config.aws_ec2_remote_config_path])))

    stdout_thread = threading.Thread(target=worker_stdout_read_thread, args=(output_queue, chan))
    stdout_thread.daemon = True
    stdout_thread.start()

    stderr_thread = threading.Thread(target=worker_stderr_read_thread, args=(processed_files_queue, input_queue, chan, ssh, abort_event))
    stderr_thread.daemon = True
    stderr_thread.start()

    return (chan, stderr_thread)

def curses_thread(config, abort_event, instances, reduce_processes, window, start_time, files_total):
    reduce_pids = [psutil.Process(x.pid) for x in reduce_processes]
    sleep_time = config.screen_refresh_interval - (config.cpu_usage_interval * len(reduce_pids))
    while not abort_event.is_set() and sleep_time > 0 and not abort_event.wait(sleep_time):
        if abort_event.is_set():
            break
        window.clear()
        now = datetime.datetime.now()
        add_str(window, 0, "smr-ec2 v{0} - {1} - elapsed: {2}".format(__version__, datetime.datetime.ctime(now), now - start_time))
        i = 1
        for instance in instances:
            add_str(window, i, "  instance {} {}".format(instance.id, instance.ip_address))
            i += 1
            for _ in xrange(config.workers):
                add_str(window, i, "    smr-map")
                i += 1
        for p in reduce_pids:
            print_pid(p, window, i, "smr-reduce")
            i += 1
        add_str(window, i + 1, "job progress: {0:%}".format(get_param("files_processed") / files_total))
        add_str(window, i + 2, "last file processed: {0}".format(get_param("last_file_processed")))
        messages = get_param("messages")[-10:]
        if len(messages) > 0:
            add_str(window, i + 3, "last messages:")
            i += 4
            for message in messages:
                add_str(window, i, "  {0}".format(message))
                i += 1
        if not abort_event.is_set():
            window.refresh()

def run(config):
    configure_job(config)
    if not config.aws_ec2_keyname:
        sys.stderr.write("invalid AWS_EC2_KEYNAME\n")
        sys.exit(1)

    print("getting list of the files to process...")
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
    print("requested to start {0} instances".format(config.aws_ec2_workers))
    instances = reservation.instances
    instance_ids = [instance.id for instance in instances]
    abort_event = threading.Event()
    initialization_threads = []
    for instance in instances:
        initialization_thread = threading.Thread(target=initialize_instance_thread, args=(config, instance, abort_event))
        initialization_thread.start()
        initialization_threads.append(initialization_thread)

    try:
        for initialization_thread in initialization_threads:
            initialization_thread.join()
    except KeyboardInterrupt:
        abort_event.set()
        conn.terminate_instances(instance_ids)
        print("user aborted. elapsed time: {0}".format(str(datetime.datetime.now() - start_time)))
        sys.exit(1)

    if abort_event.is_set():
        sys.stderr.write("could not initialize workers, terminating all instances: {0}\n".format(",".join(instance_ids)))
        conn.terminate_instances(instance_ids)
        sys.exit(1)

    print("initialized instance(s) in: {0}".format(str(datetime.datetime.now() - start_time)))
    start_time = datetime.datetime.now()

    workers = []
    for instance in instances:
        for _ in xrange(config.workers):
            workers.append(start_worker(config, instance, abort_event, output_queue, processed_files_queue, input_queue))

    reduce_stdout = open(config.output_filename, "w")
    reduce_process = subprocess.Popen(["smr-reduce"] + config.args, stdin=subprocess.PIPE, stdout=reduce_stdout)

    reduce_worker = threading.Thread(target=reduce_thread, args=(reduce_process, output_queue, abort_event))
    #reduce_worker.daemon = True
    reduce_worker.start()

    progress_worker = threading.Thread(target=progress_thread, args=(processed_files_queue, abort_event))
    #progress_worker.daemon = True
    progress_worker.start()

    if config.output_job_progress:
        window = curses.initscr()
        curses_worker = threading.Thread(target=curses_thread, args=(config, abort_event, instances, [reduce_process], window, start_time, files_total))
        #curses_worker.daemon = True
        curses_worker.start()

    try:
        for _, w in workers:
            w.join()
    except KeyboardInterrupt:
        abort_event.set()
        conn.terminate_instances(instance_ids)
        if config.output_job_progress:
            curses.endwin()
        print("user aborted. elapsed time: {0}".format(str(datetime.datetime.now() - start_time)))
        print("partial results are in {0}".format(config.output_filename))
        sys.exit(1)

    output_queue.join() # wait for reducer to process everything

    abort_event.set()
    if config.output_job_progress:
        curses_worker.join()
        curses.endwin()

    for chan, _ in workers:
        exit_code = chan.recv_exit_status()
        if exit_code != 0:
            print("map process exited with code {0}".format(exit_code))

    conn.terminate_instances(instance_ids)

    # wait for reduce to finish before exiting
    reduce_worker.join()
    reduce_process.wait()
    if reduce_process.returncode != 0:
        print("reduce process {0} exited with code {1}".format(reduce_process.pid, reduce_process.returncode))
        print("partial results are in {0}".format(config.output_filename))

    reduce_stdout.close()
    for message in get_param("messages"):
        print(message)

    print("done. elapsed time: {0}".format(str(datetime.datetime.now() - start_time)))
    print("results are in {0}".format(config.output_filename))

def main():
    config = get_config()
    run(config)
