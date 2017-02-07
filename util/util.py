import sys
from datetime import datetime
from multiprocessing import current_process
import traceback


def cout(msg):
    sys.stdout.write('[%s][%s] %s\n' % (current_process().name, datetime.now(), msg))
    sys.stdout.flush()

def with_stacktrace(func):
    import functools

    @functools.wraps(func)
    def wrapper(args):
        try:
            if isinstance(args, (str, bytes)):
                return func(args)
            else:
                return func(*args)
        except:
            proc_name = current_process().name
            for line in traceback.format_exc().splitlines():
                print('[TRACE:%s] %s' % (proc_name, line))
            raise
    return wrapper
