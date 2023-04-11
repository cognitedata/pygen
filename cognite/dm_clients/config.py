import logging
import os
import sys
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

PWD = Path().absolute()

CONFIG_FILE = os.environ.get("DM_CLIENTS_CONFIG", PWD / "config.yaml")

__all__ = [
    "CONFIG",
    "PWD",
]

test_config = {
    "dm_clients": {
        "max_tries": 1,
    }
}


# TODO since this is in an installable package now, we should refactor to remove the need for a config file.
try:
    with open(CONFIG_FILE) as f:
        CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    if "pytest" in sys.modules:
        CONFIG = test_config
    else:
        logger.error("config.yaml not found. Please see README.md to setup information.")
        sys.exit(1)
