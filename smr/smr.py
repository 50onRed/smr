#!/usr/bin/env python
import boto
import datetime
import logging
import multiprocessing
import os
import subprocess
import sys
import threading

from .config import get_config, configure_logging

def worker_process(config_name, input_queue, output_queue, processed_files_queue):
    pid = os.getpid()
    try:
        while not input_queue.empty():
            file_name = input_queue.get()
            p = subprocess.Popen(["smr-map", config_name], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            p.stdin.write("%s" % file_name)
            p.stdin.close()
            for line in p.stdout:
                output_queue.put(line)
            processed_files_queue.put(file_name)
            logging.debug("worker %d processed %s", pid, file_name)
            input_queue.task_done()
    except (KeyboardInterrupt, SystemExit):
        p.terminate()

def reduce_thread(reduce_process, output_queue):
    while True:
        result = output_queue.get()
        reduce_process.stdin.write(result)

def progress_thread(processed_files_queue, files_total):
    files_processed = 0
    while True:
        file_name = processed_files_queue.get()
        logging.debug("master received signal that %s is processed", file_name)
        files_processed += 1
        sys.stderr.write("\rprocessed {0:%}".format(files_processed / float(files_total)))

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

    workers = []
    for i in xrange(config.NUM_WORKERS):
        w = multiprocessing.Process(target=worker_process, args=(config_name, input_queue, output_queue, processed_files_queue))
        #w.daemon = True
        w.start()
        workers.append(w)

    reduce_stdout = None
    if config.OUTPUT_FILENAME is not None:
        reduce_stdout = open(config.OUTPUT_FILENAME, "w")
    reduce_process = subprocess.Popen(["smr-reduce", config_name], stdin=subprocess.PIPE, stdout=reduce_stdout)

    reduce_worker = threading.Thread(target=reduce_thread, args=(reduce_process, output_queue))
    reduce_worker.daemon = True
    reduce_worker.start()

    progress_worker = threading.Thread(target=progress_thread, args=(processed_files_queue, files_total))
    progress_worker.daemon = True
    progress_worker.start()

    try:
        input_queue.join()
    except KeyboardInterrupt:
        for worker in workers:
            worker.terminate()
        reduce_process.stdin.close()
        if reduce_stdout is not None:
            reduce_stdout.close()
        sys.stderr.write("\ruser aborted. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
        sys.stderr.write("partial results are in %s\n" % ("STDOUT" if config.OUTPUT_FILENAME is None else config.OUTPUT_FILENAME))
        sys.exit(1)

    reduce_process.stdin.close()
    sys.stderr.write("\rdone. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
    sys.stderr.write("results are in %s\n" % ("STDOUT" if config.OUTPUT_FILENAME is None else config.OUTPUT_FILENAME))
