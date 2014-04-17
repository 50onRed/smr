#!/usr/bin/env python
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import curses
import datetime
import psutil
from Queue import Queue
import subprocess
import sys
import threading

from .version import __version__
from .config import get_config, configure_job
from .shared import reduce_thread, progress_thread, write_file_to_descriptor, print_pid, get_param, add_message, add_str
from .uri import get_uris

def worker_stdout_read_thread(output_queue, map_process, abort_event):
    check_map_process(map_process, abort_event)
    for line in iter(map_process.stdout.readline, ""):
        output_queue.put(line)
    map_process.wait()

def check_map_process(map_process, abort_event):
    map_process.poll()
    if map_process.returncode is not None:
        abort_event.set()

def worker_stderr_read_thread(processed_files_queue, input_queue, map_process, abort_event):
    check_map_process(map_process, abort_event)

    # write first file to mapper
    if not abort_event.is_set() and not write_file_to_descriptor(input_queue, map_process.stdin):
        abort_event.set()
        return

    for line in iter(map_process.stderr.readline, ""):
        line = line.rstrip() # remove trailing linebreak
        if line.startswith("+"):
            file_name = line[1:]
            processed_files_queue.put(file_name)
        elif line.startswith("!"):
            input_queue.put(line[1:]) # re-queue file
        else:
            add_message("invalid message received from mapper: {0}".format(line))

        if abort_event.is_set() or not write_file_to_descriptor(input_queue, map_process.stdin):
            break

        check_map_process(map_process, abort_event)

    map_process.wait()

def curses_thread(config, abort_event, map_processes, reduce_processes, window, start_time, files_total):
    map_pids = [psutil.Process(x.pid) for x in map_processes]
    reduce_pids = [psutil.Process(x.pid) for x in reduce_processes]
    sleep_time = config.screen_refresh_interval - (config.cpu_usage_interval * (len(map_pids) + len(reduce_pids)))
    while not abort_event.is_set() and sleep_time > 0 and not abort_event.wait(sleep_time):
        if abort_event.is_set():
            break
        window.clear()
        now = datetime.datetime.now()
        add_str(window, 0, "smr v{0} - {1} - elapsed: {2}".format(__version__, datetime.datetime.ctime(now), now - start_time))
        i = 1
        for p in map_pids:
            print_pid(p, window, i, "smr-map")
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
    print("getting list of the files to process...")
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
    for _ in xrange(config.workers):
        map_process = subprocess.Popen(["smr-map"] + config.args, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        map_processes.append(map_process)

        row = threading.Thread(target=worker_stdout_read_thread, args=(output_queue, map_process, abort_event))
        row.daemon = True
        row.start()

        rew = threading.Thread(target=worker_stderr_read_thread, args=(processed_files_queue, input_queue, map_process, abort_event))
        rew.daemon = True
        rew.start()
        read_workers.append(rew)

    reduce_stdout = open(config.output_filename, "w")
    reduce_process = subprocess.Popen(["smr-reduce"] + config.args, bufsize=0, stdin=subprocess.PIPE, stdout=reduce_stdout)

    reduce_worker = threading.Thread(target=reduce_thread, args=(reduce_process, output_queue, abort_event))
    #reduce_worker.daemon = True
    reduce_worker.start()

    progress_worker = threading.Thread(target=progress_thread, args=(processed_files_queue, abort_event))
    #progress_worker.daemon = True
    progress_worker.start()

    if config.output_job_progress:
        window = curses.initscr()
        curses_worker = threading.Thread(target=curses_thread, args=(config, abort_event, map_processes, [reduce_process], window, start_time, files_total))
        #curses_worker.daemon = True
        curses_worker.start()

    try:
        for w in read_workers:
            w.join()
    except KeyboardInterrupt:
        abort_event.set()
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

    for map_process in map_processes:
        if map_process.returncode != 0:
            print("map process {0} exited with code {1}".format(map_process.pid, map_process.returncode))
            print("partial results are in {0}".format(config.output_filename))

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
