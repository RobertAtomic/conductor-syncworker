"""Conductor Upload"""

import multiprocessing
import binascii

from googleapiclient.errors import HttpError
from conductor.computeprovider.gce import util as gce_util

from . import util
from .base import ConductorBase, ConductorListBase


def _stat_object(path):
    """Stat object in GCS"""
    fields = 'size'
    gcs = util.get_gcs()
    request = gcs.objects().get(
        bucket=gce_util.get_bucket(),
        object=path,
        fields=fields
    )
    try:
        response = request.execute()
        return {'name': path,
                'size': int(response['size'])}
    except HttpError, err:
        result = {'name': path,
                  'size': 0,
                  'error': err.resp.status}
        return result


class Upload(ConductorBase):
    """Conductor Upload"""
    def __init__(self, upload_id, base_uri=None, token=None):
        """
        params:
            upload_id:
                Unique id of Upload object
        """
        super(Upload, self).__init__(
            kind='upload',
            key=upload_id,
            base_uri=base_uri,
            token=token
        )

    def decompose(self, size=35):
        """
        Break the upload object into pieces of approximately 'size' GB
        """
        shard_size_bytes = size*10**9
        items = sorted(
            self.objects, key=lambda k: int(k['size']), reverse=True
            )
        shards = []
        while items:
            shard = []
            shard_size = 0
            while shard_size < shard_size_bytes and items:
                if shard_size + int(items[-1]['size']) > \
                                    shard_size_bytes and shard_size > 0:
                    break
                shard.append(items.pop())
                shard_size += int(shard[-1]['size'])
            shards.append(shard)
        return shards

    @property
    def size(self):
        """The actual size required to sync this Upload object.
        This may be different than the various *_size attributes stored on the
        entity"""
        return sum([x['size'] for x in self.objects])

    @property
    def objects(self):
        """List of GCS objects in this Upload"""
        if not hasattr(self, '_objects'):
            self._objects = None
        if not self._objects:
            pool = multiprocessing.Pool(multiprocessing.cpu_count()*4)
            object_list = ['accounts/{}/hashstore/{}'
                           .format(self.account,
                                   binascii.b2a_hex(binascii.a2b_base64(x)))
                           for x in set(self.upload_files.values())]
            self._objects = pool.map(_stat_object, object_list)
        return self._objects

    @property
    def ready(self):
        """Check if all objects are present in GCS.
        This forces a fetch from GCS so it should be called sparingly"""
        self._objects = None
        return not any(['error' in x for x in self.objects])

    @property
    def stats(self):
        """Return some sizes stats"""
        return util.stats(self.objects)


class UploadList(ConductorListBase):
    """List of Conductor Upload objects"""
    def __init__(self, filters=None):
        super(UploadList, self).__init__(
            kind='Upload',
            filters=filters
        )

    @property
    def list(self):
        self._fetch()
        for i in range(len(self._list)):
            self._list[i].__class__ = Upload
        return self._list
