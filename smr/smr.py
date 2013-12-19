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

def reduce_process(config_name, output_queue):
    config = get_config(config_name)
    stdout = None
    if config.OUTPUT_FILENAME is not None:
        stdout = open(config.OUTPUT_FILENAME % {"config_name": os.path.basename(config_name), "time": datetime.datetime.now()}, "w")
    p = subprocess.Popen(["smr-reduce", config_name], stdin=subprocess.PIPE, stdout=stdout)
    try:
        while True:
            result = output_queue.get()
            p.stdin.write(result)
    except (KeyboardInterrupt, SystemExit):
        p.stdin.close()
        if stdout is not None:
            stdout.close()

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

    reduce_worker = multiprocessing.Process(target=reduce_process, args=(config_name, output_queue))
    reduce_worker.start()

    progress_worker = threading.Thread(target=progress_thread, args=(processed_files_queue, files_total))
    progress_worker.daemon = True
    progress_worker.start()

    try:
        input_queue.join()
    except KeyboardInterrupt:
        for worker in workers:
            worker.terminate()
        sys.stderr.write("\ruser aborted. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
        reduce_worker.terminate()
        sys.exit(1)
    
    # cleanup, kill all processes
    sys.stderr.write("\rdone. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
    reduce_worker.terminate()
