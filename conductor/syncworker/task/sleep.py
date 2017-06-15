"""
Sleep task
    A simple test task to waste some time
"""

import time

from .. import app


@app.task
def sleep(delay):
    """
    A test task
    """
    time.sleep(delay)
    return 0
