#!/usr/bin/env python
import curses
import datetime
import logging
import psutil
from Queue import Queue
import subprocess
import sys
import threading

from .shared import get_config, reduce_thread, progress_thread, write_file_to_descriptor, curses_thread
from .uri import get_uris
from . import __version__

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

    p = psutil.Process(map_process.pid)

    for line in iter(map_process.stderr.readline, ""):
        line = line.rstrip() # remove trailing linebreak
        try:
            cpu_percent = p.get_cpu_percent(1.0)
        except:
            cpu_percent = 0.0
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

def end_curses(window):
    window.keypad(0)
    curses.nocbreak()
    curses.echo()
    curses.endwin()

def main():
    config = get_config()
    print "logging to %s" % (config.log_filename)

    window = curses.initscr()
    curses.noecho()
    curses.curs_set(0)
    window.keypad(1)

    file_names = get_uris(config)
    files_total = len(file_names)

    input_queue = Queue(files_total)
    for file_name in file_names:
        input_queue.put(file_name)
    output_queue = Queue()
    processed_files_queue = Queue(files_total)

    start_time = datetime.datetime.now()
    abort_event = threading.Event()

    map_processes = []
    read_workers = []
    for i in xrange(config.workers):
        map_process = subprocess.Popen(["smr-map"] + sys.argv[1:], bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        map_processes.append(map_process)

        row = threading.Thread(target=worker_stdout_read_thread, args=(output_queue, map_process, abort_event))
        row.daemon = True
        row.start()

        rew = threading.Thread(target=worker_stderr_read_thread, args=(processed_files_queue, input_queue, map_process, abort_event))
        rew.daemon = True
        rew.start()
        read_workers.append(rew)

    reduce_stdout = open(config.output_filename, "w")
    reduce_process = subprocess.Popen(["smr-reduce"] + sys.argv[1:], bufsize=0, stdin=subprocess.PIPE, stdout=reduce_stdout)

    reduce_worker = threading.Thread(target=reduce_thread, args=(reduce_process, output_queue, abort_event))
    #reduce_worker.daemon = True
    reduce_worker.start()

    progress_worker = threading.Thread(target=progress_thread, args=(processed_files_queue, abort_event))
    #progress_worker.daemon = True
    progress_worker.start()

    curses_worker = threading.Thread(target=curses_thread, args=(abort_event, map_processes, [reduce_process], window, start_time))
    #curses_worker.daemon = True
    curses_worker.start()

    try:
        for w in read_workers:
            w.join()
    except KeyboardInterrupt:
        abort_event.set()
        print "user aborted. elapsed time: {0}".format(str(datetime.datetime.now() - start_time))
        print "partial results are in {0}".format(config.output_filename)
        end_curses(window)
        sys.exit(1)

    abort_event.set()
    # wait for reduce to finish before exiting
    reduce_worker.join()
    reduce_process.wait()
    reduce_stdout.close()
    end_curses(window)
    print "done. elapsed time: {0}".format(str(datetime.datetime.now() - start_time))
    print "results are in {0}".format(config.output_filename)
