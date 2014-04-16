__version__ = '0.0.1'
__all__ = ["run", "run_ec2", "run_map", "run_reduce", "get_config", "get_default_config"]

from .main import run
from .ec2 import run as run_ec2
from .map import run as run_map
from .reduce import run as run_reduce
from .config import get_config, get_default_config
