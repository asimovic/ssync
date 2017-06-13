######################################################################
#
# File: sync/folder_parser.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
# Modified by: Alex Simovic
#
######################################################################

import util
from b2.exception import CommandError
from index.secure_index_factory import SecureIndexFactory
from .folder import LocalFolder, SecureFolder


def parseSyncDir(dirPath, conf, api):
    """
    Takes either a local path, or a B2 path, and returns a Folder object for it.

    B2 paths look like: b2://bucketName/path/name.
    Anything else is treated like a local folder.
    """
    if dirPath.startswith('b2://'):
        return __parseSecureB2Folder(dirPath, conf, api)
    else:
        return LocalFolder(dirPath)


def __parseB2Folder(bucket_and_path, api):
    """
    Turns 'my-bucket/foo' into B2Folder(my-bucket, foo)
    """
    if '//' in bucket_and_path:
        raise CommandError("'//' not allowed in path names")
    if '/' not in bucket_and_path:
        bucket_name = bucket_and_path
        folder_name = ''
    else:
        (bucket_name, folder_name) = bucket_and_path.split('/', 1)
    if folder_name.endswith('/'):
        folder_name = folder_name[:-1]
    return B2Folder(bucket_name, folder_name, api)


def __parseSecureB2Folder(path, conf, api):
    """
    Turns 'b2://my-bucket/foo' into a secure folder
    """
    if not path.startswith('b2://'):
        raise CommandError("invalid b2 path, should start with 'b2://': " + path)
    path = util.normalizePath(path[5:], True)
    if '//' in path:
        raise CommandError("'//' not allowed in path names")
    if path.count('/', 0, -1) == 0:
        bucketName = path
        folderName = ''
    else:
        (bucketName, folderName) = path.split('/', 1)

    sif = SecureIndexFactory(conf, api, bucketName)
    s = sif.getIndex()

    return SecureFolder(folderName, s)