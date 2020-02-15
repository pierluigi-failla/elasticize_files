# -*- coding: utf-8 -*-
"""
Created by Pierluigi on 2020-01-29
project: elasticizefiles
"""
import hashlib
import os
from platform import system
from re import findall
from socket import gethostbyname
from socket import gethostname
from uuid import getnode


def explore_path(path, recursive=True):
    """ Navigate a given path returning all files in folder and subfolders

    :params path: a path to a folder
    :params recursive: recursively explore subfolder (default `True`)
    :returns: a generator of pairs: (`dirname`, `filename`)
    """
    for dirname, _, filenames in os.walk(path):
        for filename in filenames:
            yield dirname, filename
        if not recursive:
            break


def filestat(filename):
    """ Get file stats and convert it to a dict.

    :params filename: a filename comprehensive of path if necessary
    :returns: a dictionary see [1] for more details
    [1] https://docs.python.org/3.6/library/os.html#os.stat_result
    """
    fs = os.stat(filename)
    return {k: getattr(fs, k) for k in dir(fs) if k.startswith('st_')}


def get_hash(filename, hash_type='sha256'):
    """ Get hash of binary files

    :params filename: a filename comprehensive of path if necessary
    :params hash_type: an hashing function from hashlib
    :returns: a string containing the digest
    """
    func = getattr(hashlib, hash_type)()
    f = os.open(filename, (os.O_RDWR | os.O_BINARY))
    for block in iter(lambda: os.read(f, 2048 * func.block_size), b''):
        func.update(block)
    os.close(f)
    return func.hexdigest()


def get_machine_info():
    """ Collect few info about phisical machine. """
    return {
        'platform': system(),
        'hostname': gethostname(),
        'ip_address': gethostbyname(gethostname()),
        'mac_address': ':'.join(findall('..', '%012x' % getnode())),
    }
