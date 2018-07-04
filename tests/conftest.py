import logging
import os
import sys

logging.basicConfig(level=os.environ.get('LOGLEVEL', logging.WARNING), stream=sys.stdout)
