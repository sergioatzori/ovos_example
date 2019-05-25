#!/usr/bin/python3

from vars import log_file, socket_port
import logging
import os
from server import MessageServer

if os.path.exists(log_file):
    os.remove(log_file)

logging.basicConfig(level=logging.DEBUG, filename=log_file)

MessageServer(socket_port).start()
