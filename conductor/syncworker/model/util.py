"""
Helper functions
"""

import os
import math
import statistics

from apiclient import discovery
from oauth2client.client import GoogleCredentials


def check_retry(exc):
    """Decide whether a call should be retried based on exception"""
    # TODO: decide which exceptions to retry
    return True


def get_stat(func, data):
    """Call statistics function that might raise
    an exception and return result or None"""
    res = None,
    try:
        res = func(data)
    except statistics.StatisticsError:
        pass
    return res


def stats(items):
    """Return some basic statistics of Hashstore or Upload object"""
    sizes = [int(x['size']) for x in items]
    data = {
        'count': len(sizes),
        'total': sum(sizes),
        'min': min(sizes),
        'max': max(sizes),
        'mean': get_stat(statistics.mean, sizes),
        'stdev': get_stat(statistics.stdev, sizes),
        'variance': get_stat(statistics.variance, sizes),
        'mode': get_stat(statistics.mode, sizes)
    }
    return data


def to_gb(size):
    """Convert bytes to Gigabytes rounding up as necesarry"""
    return int(math.ceil(float(size) / 10**9))


def get_token():
    """Return conductor token"""
    if 'CONDUCTOR_TOKEN' in os.environ:
        return os.environ['CONDUCTOR_TOKEN']
    else:
        raise ValueError(
            """Failed to get conductor API token.  Set CONDUCTOR_TOKEN in
            the environment."""
            )


def get_account():
    """Return conductor account"""
    if 'CONDUCTOR_ACCOUNT' in os.environ:
        return os.environ['CONDUCTOR_ACCOUNT']
    else:
        raise ValueError(
            """Failed to get conductor account.  Set CONDUCTOR_ACCOUNT in
            the environment."""
            )


def get_uri():
    """Return base uri for Conductor API calls"""
    if 'CONDUCTOR_URI' in os.environ:
        return os.environ['CONDUCTOR_URI']
    else:
        raise ValueError(
            """Failed to get Conductor base URI.  Set CONDUCTOR_URI in the
            environment."""
            )


def get_gcs():
    """Return handle to GCS service"""
    return discovery.build(
        'storage',
        'v1',
        credentials=GoogleCredentials.get_application_default()
        )
