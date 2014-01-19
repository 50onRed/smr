#!/usr/bin/env python
import datetime
import logging
from Queue import Queue
import subprocess
import sys
import threading

from .shared import get_config, configure_logging, get_files_to_process, reduce_thread, \
    progress_thread, write_file_to_descriptor

def worker_stdout_read_thread(output_queue, map_process, abort_event):
    check_map_process(map_process, abort_event)
    for line in iter(map_process.stdout.readline, ""):
        output_queue.put(line)
    map_process.wait()

def check_map_process(map_process, abort_event):
    map_process.poll()
    if map_process.returncode is not None:
        abort_event.set()
        logging.error("map process %d exited with code %d", map_process.pid, map_process.returncode)
        sys.exit(1)

def worker_stderr_read_thread(processed_files_queue, input_queue, map_process, abort_event):
    check_map_process(map_process, abort_event)

    # write first file to mapper
    if not abort_event.is_set() and not write_file_to_descriptor(input_queue, map_process.stdin):
        abort_event.set()
        logging.error("map process %d exited with code %d", map_process.pid, map_process.returncode)
        sys.exit(1)

    for line in iter(map_process.stderr.readline, ""):
        line = line.rstrip() # remove trailing linebreak
        if line.startswith("+"):
            processed_files_queue.put(line[1:])
        elif line.startswith("!"):
            input_queue.put(line[1:]) # re-queue file
        else:
            logging.error("invalid message received from mapper: %s", line)

        if abort_event.is_set() or not write_file_to_descriptor(input_queue, map_process.stdin):
            break

        check_map_process(map_process, abort_event)

    map_process.wait()

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr config.py\n")
        sys.exit(1)

    config_name = sys.argv[1]
    config = get_config(config_name)

    configure_logging(config)
    print "logging to %s" % (config.LOG_FILENAME)

    file_names = get_files_to_process(config)
    files_total = len(file_names)

    input_queue = Queue(files_total)
    for file_name in file_names:
        input_queue.put(file_name)
    output_queue = Queue()
    processed_files_queue = Queue(files_total)

    start_time = datetime.datetime.now()
    abort_event = threading.Event()

    read_workers = []
    for i in xrange(config.NUM_WORKERS):
        map_process = subprocess.Popen(["smr-map", config_name], bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        row = threading.Thread(target=worker_stdout_read_thread, args=(output_queue, map_process, abort_event))
        row.daemon = True
        row.start()

        rew = threading.Thread(target=worker_stderr_read_thread, args=(processed_files_queue, input_queue, map_process, abort_event))
        rew.daemon = True
        rew.start()
        read_workers.append(rew)

    reduce_stdout = open(config.OUTPUT_FILENAME, "w")
    reduce_process = subprocess.Popen(["smr-reduce", config_name], bufsize=0, stdin=subprocess.PIPE, stdout=reduce_stdout)

    reduce_worker = threading.Thread(target=reduce_thread, args=(reduce_process, output_queue, abort_event))
    #reduce_worker.daemon = True
    reduce_worker.start()

    progress_worker = threading.Thread(target=progress_thread, args=(processed_files_queue, files_total, abort_event))
    #progress_worker.daemon = True
    progress_worker.start()

    try:
        for w in read_workers:
            w.join()
    except KeyboardInterrupt:
        abort_event.set()
        sys.stderr.write("\ruser aborted. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
        sys.stderr.write("partial results are in %s\n" % (config.OUTPUT_FILENAME))
        sys.exit(1)

    abort_event.set()
    reduce_stdout.close()
    sys.stderr.write("\rdone. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
    sys.stderr.write("results are in %s\n" % (config.OUTPUT_FILENAME))
