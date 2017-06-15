"""Local disk utilities"""

import os
import stat
import errno

from .process_util import sudo_subprocess


def create_mount_dir(mount_dirpath):
    """Creates directory for mounting filesystem.
    arguments:
        mount_dirpath: filesystem path to create.
    """
    if not os.path.exists(mount_dirpath):
        args = ['/bin/mkdir', '-p', mount_dirpath]
        sudo_subprocess(args)


def remove_mount_dir(mount_dirpath):
    """Remove directory
    arguments:
        mount_dirpath:
            filesystem path to remove
    """
    if os.path.exists(mount_dirpath):
        args = ['/bin/rm', '-rf', mount_dirpath]
        sudo_subprocess(args)


def get_device(device_name):
    """Accepts either GCE freindly device name or raw device path.  returns raw device path
       arguments:
           device_name: string (user friendly device name e.g. job_atomicfiction_10204
                        or full device name e.g. /dev/sdb1)
    """
    try:
        mode = os.stat(device_name).st_mode
        if stat.S_ISBLK(mode):
            # This is a full path to a block device already
            return device_name
    except OSError as exc:
        if not exc.errno == errno.ENOENT:
            raise
    # Try again with GCE decorated name
    gce_path = '/dev/disk/by-id/google-{}'.format(device_name)
    return gce_path


def chmod(path, mode):
    """Wrapper for chmod command"""
    args = ['/bin/chmod', mode, path]
    sudo_subprocess(args)


def chown(path, owner):
    """Wrapper for chown command"""
    args = ['/bin/chown', owner, path]
    sudo_subprocess(args)


def umount(device_name, cleanup=False):
    """Unmounts a filesystem
    arguments:
        device_name: string (user friendly device name e.g. job_atomicfiction_10204
                     or full device name e.g. /dev/sdb1 or path to mount point e.g. /mnt/data)
    """
    if os.path.ismount(device_name):
        device = device_name
    else:
        device = get_device(device_name)
        args = ['/bin/umount', device]
        sudo_subprocess(args)


def mount(device_name, mountpoint):
    """Mounts specified device on specified mountpoint
    Mountpoint path will be created if necesarry
    arguments:
        device_name: string (user friendly device name e.g. job_atomicfiction_10204
                     or full device name e.g. /dev/sdb1)
        mountpoint: string (path to mount e.g. /mnt/data)
    """
    create_mount_dir(mountpoint)
    device = get_device(device_name)
    args = ['/bin/mount', '-o', 'discard,defaults', device, mountpoint]
    sudo_subprocess(args)
    chmod(mountpoint, '777')


def mount_aufs(mountpoint, branches):
    """Mounts specified braches on specified mountpoint.
    Mountpoint path will be created if necesarry
    arguments:
        mountpoint: string (path to mount e.g. /mnt/data)
        branches: list of dict (dict contains {'path':'/path/to/branch','mode':'rw|ro|rr'})
    """
    create_mount_dir(mountpoint)
    options = 'br='
    for branch in branches:
        branch_str = '='.join((branch['path'], branch['mode'])) + ":"
        options += branch_str
    options = options[:-1]
    args = ['/bin/mount', '-t', 'aufs', '-o', options, 'none', mountpoint]
    sudo_subprocess(args)


def format_disk(device_name):
    """WARNING: this will destroy data.  Only format empty disks.
    Formats specified device
    arguments:
        device_name: string (user friendly device name e.g. job_atomicfiction_10204
                     or full device name e.g. /dev/sdb1)
    """
    device = get_device(device_name)
    args = ['/sbin/mkfs.ext4', '-F', '-E', "lazy_itable_init=0,lazy_journal_init=0,discard", device]
    sudo_subprocess(args)
