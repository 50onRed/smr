#!/usr/bin/env python
import datetime
import logging
import multiprocessing
from Queue import Empty, Queue
import subprocess
import sys
import threading

from .shared import get_config, configure_logging, get_files_to_process, reduce_thread, progress_thread

def worker_thread(config_name, input_queue, output_queue, processed_files_queue, abort_event):
    while not abort_event.is_set():
        try:
            file_name = input_queue.get(timeout=2)
            # TODO: possibly suppress stderr of map_process
            map_process = subprocess.Popen("echo '%s' | smr-map %s" % (file_name, config_name), stdout=subprocess.PIPE, shell=True)
            for line in map_process.stdout:
                output_queue.put(line)
            map_process.wait()
            if map_process.returncode != 0:
                logging.error("echo '%s' | smr-map %s", file_name, config_name)
                logging.error("map process %d exited with code %d", map_process.pid, map_process.returncode)
                input_queue.task_done()
                input_queue.put_nowait(file_name) # requeue file
                continue
            processed_files_queue.put(file_name)
            logging.debug("worker %d processed %s", map_process.pid, file_name)
            input_queue.task_done()
        except Empty:
            pass

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: smr config.py\n")
        sys.exit(1)

    config_name = sys.argv[1]
    config = get_config(config_name)

    configure_logging(config)

    file_names = get_files_to_process(config)
    files_total = len(file_names)

    input_queue = multiprocessing.JoinableQueue(files_total)
    for file_name in file_names:
        input_queue.put(file_name)
    output_queue = Queue()
    processed_files_queue = Queue()

    start_time = datetime.datetime.now()
    abort_event = threading.Event()

    workers = []
    for i in xrange(config.NUM_WORKERS):
        w = threading.Thread(target=worker_thread, args=(config_name, input_queue, output_queue, processed_files_queue, abort_event))
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
        sys.stderr.write("\ruser aborted. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
        sys.stderr.write("partial results are in %s\n" % (config.OUTPUT_FILENAME))
        sys.exit(1)

    abort_event.set()
    reduce_stdout.close()
    sys.stderr.write("\rdone. elapsed time: %s\n" % str(datetime.datetime.now() - start_time))
    sys.stderr.write("results are in %s\n" % (config.OUTPUT_FILENAME))
