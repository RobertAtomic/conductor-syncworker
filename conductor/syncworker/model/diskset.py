"""Diskset operations"""

import uuid
import random
import string

from conductor.computeprovider.gce import Disk, DiskList


class Diskset(object):
    """"Conductor diskset"""
    def __init__(self, account, zone, diskset_id=None):
        """
        params:
            account:
                string
            diskset_id:
                identity of existing disket or create a new identity
        """
        self.account = account

        if diskset_id:
            self.diskset_id = diskset_id
        else:
            self.diskset_id = 'ds' + uuid.uuid4().hex
        self.zone = zone

    def _fetch(self):
        """fetch disk info from provider"""
        query_filter = '(labels.diskset eq {})(labels.account eq {})'\
            .format(self.diskset_id, self.account)
        return DiskList(zone=self.zone, query_filter=query_filter)

    def add_disk(self, count=1, size=500, base_name=None,
                 disk_type='pd-standard', snapshot=None,
                 wait=False):
        """Add disks to set
        params:
            count:
                number of disks to add to set
            size:
                Size of each disk to add
            base_name:
                Disks are named by appending a hyphen and a random
                four-character string to the base_name. defaults to
                account+diskset_id
            disk_type:
                type of disk to create.  must be either of 'pd-standard' or
                'pd-ssd'.
            snapshot:
                create disk from snapshot.
            wait:
                if True wait for all operations to complete.  These will be run
                in series.  Caller should implement timeout functionality.  If
                wait is True any failures will raise RuntimeError exception
                which could lead to a partial completeion.  The caller should
                cleanup if necesary.
        returns:
            List of Operation objects
        """
        if not base_name:
            base_name = self.account + '-' + self.diskset_id
        ops = []
        for _ in xrange(count):
            disk_name = base_name + '-' \
                + ''.join(random.choice(string.lowercase) for i in range(4))
            disk = Disk(name=disk_name, zone=self.zone)
            ops.append(
                disk.create(size=size,
                            disk_type=disk_type,
                            snapshot=snapshot,
                            wait=wait)
            )
            ops.append(
                disk.set_labels({'diskset': self.diskset_id,
                                 'account': self.account},
                                wait=wait)
            )
        return ops

    def delete(self, wait=False):
        """Deletes all disks in diskset
        params:
            wait:
                If True wait for all operations to complete.  These will be run
                in series.  Caller should implement timeout functionality.  If
                wait is True any failures will raise RuntimeError exception
                which could lead to a partial completion.  The caller should
                cleanup if necessary.
        """
        ops = []
        for disk in self.disks:
            ops.append(disk.delete(wait=wait))
        return ops

    def __getitem__(self, idx):
        return self.disks[idx]

    def __len__(self):
        return len(self.disks)

    @property
    def disks(self):
        """List of Disks"""
        return self._fetch()


class DisksetList(object):
    """List of Diskset"""
    def __init__(self, zone):
        self.zone = zone

    def _fetch(self):
        query_filter = 'labels.diskset eq .*'
        disksets = []
        disks = DiskList(zone=self.zone, query_filter=query_filter)
        for disk in disks:
            if not disk.labels['diskset'] in disksets:
                disksets.append(Diskset(diskset_id=disk.labels['diskset'],
                                        account=disk.labels['account'],
                                        zone=self.zone))
        return disksets

    def __getitem__(self, idx):
        return self.disksets[idx]

    def __len__(self):
        return len(self.disksets)

    @property
    def disksets(self):
        """List of disksets"""
        return self._fetch()
