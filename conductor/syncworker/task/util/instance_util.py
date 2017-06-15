"""
GCE instance utility functions
"""

import requests

from conductor.computeprovider.gce import util as gcp_util
from conductor.computeprovider.gce import Instance


def is_gce():
    """
    Check if caller is running ona GCE instance.
    """
    try:
        gcp_util.get_metadata()
    except requests.RequestException:
        return False
    return True


def get_this_instance():
    """
    Return Instance object representing this instance.
    Fails if not running on a GCE instance
    """
    if is_gce():
        return Instance(
            gcp_util.get_metadata()['instance']['name'],
            gcp_util.get_metadata()['instance']['zone'].split('/')[-1]
            )
    else:
        raise RuntimeError('Not running on GCE')
