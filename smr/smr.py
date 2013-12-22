#!/usr/bin/env python
import boto
import datetime
import logging
import multiprocessing
from Queue import Empty
import subprocess
import sys
import threading

from .config import get_config, configure_logging

def worker_thread(config_name, input_queue, output_queue, processed_files_queue, abort_event):
    # assume that input_queue is pre-filled and never empty until the end
    while not abort_event.is_set():
        try:
            file_name = input_queue.get(timeout=2)
            # TODO: possibly suppress stderr of map_process
            map_process = subprocess.Popen(["smr-map", config_name], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            if map_process.poll() is not None:
                logging.error("map process %d exited with code %d", map_process.pid, map_process.returncode)
                input_queue.put(file_name) # requeue file
                input_queue.task_done()
                continue
            map_process.stdin.write("%s" % file_name)
            map_process.stdin.close()
            for line in map_process.stdout:
                output_queue.put(line)
            processed_files_queue.put(file_name)
            logging.debug("worker %d processed %s", map_process.pid, file_name)
            input_queue.task_done()
        except Empty:
            pass

def reduce_thread(reduce_process, output_queue, abort_event):
    while True:
        if abort_event.is_set():
            reduce_process.terminate()
            break
        try:
            result = output_queue.get(timeout=2)
            if reduce_process.poll() is not None:
                # don't want to write if process has already terminated
                logging.error("reduce process %d ended with code %d", reduce_process.pid, reduce_process.returncode)
                abort_event.set()
                break
            reduce_process.stdin.write(result)
        except Empty:
            pass

def progress_thread(processed_files_queue, files_total, abort_event):
    files_processed = 0
    while not abort_event.is_set():
        try:
            file_name = processed_files_queue.get(timeout=2)
            logging.debug("master received signal that %s is processed", file_name)
            files_processed += 1
            sys.stderr.write("\rprocessed {0:%}".format(files_processed / float(files_total)))
        except Empty:
            pass

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr config.py\n")
        sys.exit(1)

    config_name = sys.argv[1]
    config = get_config(config_name)

    configure_logging(config)

    file_names = []
    logging.info("getting list of files from s3...")
    s3conn = boto.connect_s3(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    bucket = s3conn.get_bucket(config.S3_BUCKET_NAME)
    for prefix in config.S3_FILE_PREFIXES:
        for key in bucket.list(prefix=prefix):
            file_names.append(key.name)
    files_total = len(file_names)
    logging.info("going to process %d files...", files_total)

    input_queue = multiprocessing.JoinableQueue(files_total)
    for file_name in file_names:
        input_queue.put(file_name)
    output_queue = multiprocessing.Queue()
    processed_files_queue = multiprocessing.Queue()

    start_time = datetime.datetime.now()
    abort_event = threading.Event()

    workers = []
    for i in xrange(config.NUM_WORKERS):
        w = threading.Thread(target=worker_thread, args=(config_name, input_queue, output_queue, processed_files_queue, abort_event))
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
