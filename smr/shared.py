from __future__ import absolute_import, division, print_function, unicode_literals
import boto
import curses
import os
from Queue import Empty

GLOBAL_SHARED_DATA = {
    "files_processed": 0,
    "bytes_processed": 0,
    "last_file_processed": "",
    "messages": []
}

def reduce_thread(reduce_process, output_queue, abort_event):
    while not abort_event.is_set():
        try:
            # result has a trailing linebreak
            result = output_queue.get(timeout=2)
            if reduce_process.poll() is not None:
                # don't want to write if process has already terminated
                abort_event.set()
                break
            reduce_process.stdin.write(result)
            reduce_process.stdin.flush()
            output_queue.task_done()
        except Empty:
            pass
    # we're calling communicate() on the process, which flushes stdin
    # so we can't close it here
    #reduce_process.stdin.close()

def print_pid(process, window, line_num, process_name):
    try:
        cpu_percent = process.cpu_percent(0.1)
    except:
        cpu_percent = 0.0
    add_str(window, line_num, "  {} pid {} CPU {}".format(process_name, process.pid, cpu_percent))

def add_str(window, line_num, str):
    """ attempt to draw str on screen and ignore errors if they occur """
    try:
        window.addstr(line_num, 0, str)
    except curses.error:
        pass

def progress_thread(processed_files_queue, abort_event):
    while not abort_event.is_set():
        try:
            file_name, file_size = processed_files_queue.get(timeout=2)
            GLOBAL_SHARED_DATA["files_processed"] += 1
            GLOBAL_SHARED_DATA["bytes_processed"] += file_size
            GLOBAL_SHARED_DATA["last_file_processed"] = file_name
            processed_files_queue.task_done()
        except Empty:
            pass

def get_param(param):
    return GLOBAL_SHARED_DATA[param]

def add_message(message):
    GLOBAL_SHARED_DATA["messages"].append(message)

def write_file_to_descriptor(input_queue, descriptor):
    """
    get item from input_queue and write it to descriptor
    returns True if and only if it was successfully written
    """
    try:
        file_name = input_queue.get(timeout=2)
        descriptor.write("{}\n".format(file_name))
        descriptor.flush()
        input_queue.task_done()
        return True
    except Empty:
        # no more files in queue
        descriptor.close()
        return False
    except IOError:
        return False # probably bad descriptor

def ensure_dir_exists(path):
    dir_name = os.path.dirname(path)
    if dir_name != '' and not os.path.exists(dir_name):
        os.makedirs(dir_name)

def get_args(process, config, config_path=None):
    args = [process]

    if config.aws_access_key:
        args.append("--aws-access-key")
        args.append(config.aws_access_key)
    elif boto.config.get('Credentials', 'aws_access_key_id'):
        args.append("--aws-access-key")
        args.append(boto.config.get('Credentials', 'aws_access_key_id'))

    if config.aws_secret_key:
        args.append("--aws-secret-key")
        args.append(config.aws_secret_key)
    elif boto.config.get('Credentials', 'aws_secret_access_key'):
        args.append("--aws-secret-key")
        args.append(boto.config.get('Credentials', 'aws_secret_access_key'))

    if not config_path:
        config_path = config.config

    args.append(config_path)
    return args
