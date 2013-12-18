import os
import sys

def get_config(config_name):
    if config_name.endswith(".py"):
        config_name = config_name[:-3]
    elif config_name.endswith(".pyc"):
        config_name = config_name[:-4]

    directory, config_module = os.path.split(config_name)

    # If the directory isn't in the PYTHONPATH, add it so our import will work
    if directory not in sys.path:
        sys.path.insert(0, directory)

    config = __import__(config_module)

    return config
