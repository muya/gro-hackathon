import logging
import os
import sys

from logging import FileHandler
from logging import Formatter

LOG_FILE = "/var/log/applications/gro-hackathon/gro-hack.log"
DEBUG_FILE = LOG_FILE
LOG_FORMAT = (
    "%(asctime)s [%(levelname)s]: %(message)s in %(pathname)s:%(lineno)d")


LOG_LEVEL = logging.DEBUG

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)
file_handler = FileHandler(LOG_FILE)
file_handler.setLevel(LOG_LEVEL)
file_handler.setFormatter(Formatter(LOG_FORMAT))
logger.addHandler(file_handler)
