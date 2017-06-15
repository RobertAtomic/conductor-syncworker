"""
Conductor worker sync task
"""

import os
import re
import timeit
import time
import logging
import traceback
import signal
import multiprocessing

import boto
import gcs_oauth2_boto_plugin # pylint: disable=unused-import

from .. import app
from conductor.computeprovider.gce import util as gce_util
from conductor.computeprovider.gce import Disk
from ..model import Diskset, ConductorWorkerException
from .util import disk_util, instance_util

logger = logging.getLogger(__name__)


def abort_on_terminate(signum, frame):
    """This normally happens if the instance is pre-empted"""
    logger.info('Caught SIGTERM... retrying task')
    raise ConductorWorkerException


def progress(completed, total):
    """Report download progress"""
    logger.info('Download progress: %d/%d', completed, total)


def sync_gcs_object(mountpoint, gcs_path):
    """Copy object from gcs to local disk"""
    rex = r'^accounts/(?P<account>[\w-]*)/hashstore/(?P<name>[a-zA-Z0-9+=/]*)$'
    match = re.match(rex, gcs_path)
    if match:
        filename = match.groupdict()['name']
        filepath = os.path.join(mountpoint, filename)
        gcspath = gce_util.get_bucket() + '/' + gcs_path
        logger.info('sync %s to %s', gcspath, filepath)
        try:
            key = boto.storage_uri(gcspath, 'gs').get_key()
            key.get_contents_to_filename(filepath, cb=progress)
            key.set_remote_metadata({'conductor-atime': int(time.time())},
                                    {},
                                    True)
        except Exception:  # pylint: disable=broad-except
            logger.exception('Exception while downloading %s', gcs_path)
            return traceback.format_exc()
    else:
        logger.error('Error in gcs path: %s', gcs_path)
        return 'Error in gcs path: %s', gcs_path


@app.task()
def sync(sync_data):
    """Sync data from GCS to local disk
    arguments:
        sync_data:
            dict of the form:
                {'account': 'conductor',
                 'diskset_id': '123456789',
                 'shard': {'shard': 
                     [{'name':'accounts/conductor/hashstore/9fbf4e1bef55d3cee05c384c2ba868fe',
                       'size': 5900},
                      {'name':'accounts/conductor/hashstore/66877a1e197b8158918b8a3428bca10c',
                       'size': 102959},
                      {'name':'accounts/conductor/hashstore/af0cbca8857d5ecff0fab9ade5ee1ee3',
                       'size': 3679727}],
                 'sizeGb': 50}}
    """
    start_time = timeit.default_timer()
    signal.signal(signal.SIGTERM, abort_on_terminate)
    mountpoint, disk, instance = setup(sync_data)
    setup_time = timeit.default_timer() - start_time

    processing_start_time = timeit.default_timer()
    pool = multiprocessing.Pool(multiprocessing.cpu_count()*4)
    errors = []
    for gcs_path in [x['name'] for x in sync_data['shard']['shard']]:
        pool.apply_async(sync_gcs_object,
                         args=(mountpoint, gcs_path, ),
                         callback=errors.append)
    pool.close()
    pool.join()
    processing_time = timeit.default_timer() - processing_start_time

    cleanup_start_time = timeit.default_timer()
    cleanup(mountpoint, disk, instance)
    cleanup_time = timeit.default_timer() - cleanup_start_time

    elapsed_time = timeit.default_timer() - start_time
    logger.info('errors: %s', errors)

    return {'setup_time': setup_time,
            'processing_time': processing_time,
            'cleanup_time': cleanup_time,
            'total_time': elapsed_time,
            'disk': {'name': disk.name, 'zone': disk.zone},
            'errors': filter(None, errors),
            'size': sum([int(x['size']) for x in sync_data['shard']['shard']])}


def get_instance():
    """Return Instance object representing this instance"""
    return instance_util.get_this_instance()


def get_disk(diskset_id, size, account):
    """Create and return Disk object"""
    zone = gce_util.get_metadata()['instance']['zone'].split('/')[-1]
    diskset = Diskset(account=account,
                      zone=zone,
                      diskset_id=diskset_id)
    # Add 1Gb padding to avoid out of space errors
    op = diskset.add_disk(size=size + 1, wait=True)
    return Disk(
        name=op[0].target['name'],
        zone=zone
    )


def setup(sync_data):
    """Prepare instance to sync"""
    instance = get_instance()
    disk = get_disk(sync_data['diskset_id'],
                    sync_data['shard']['sizeGb'],
                    sync_data['account'])
    instance.attach_disk(disk, wait=True)
    disk_util.format_disk(disk.name)
    mountpoint = '/mnt/' + disk.name
    disk_util.mount(disk.name, mountpoint)
    return mountpoint, disk, instance


def cleanup(mountpoint, disk, instance):
    """Release resources and sanitize instance"""
    disk_util.umount(disk.name)
    disk_util.remove_mount_dir(mountpoint)
    instance.detach_disk(disk, wait=True)
    disk.snapshot(name=disk.name, wait=True)
    disk.delete()
