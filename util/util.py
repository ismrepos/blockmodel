import sys
from datetime import datetime
from multiprocessing import current_process
import traceback


def cout(msg):
    sys.stdout.write('[%s][%s] %s\n' % (current_process().name, datetime.now(), msg))
    sys.stdout.flush()
