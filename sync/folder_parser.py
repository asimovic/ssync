######################################################################
#
# File: sync/folder_parser.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
# Modified by: Alex Simovic
#
######################################################################

from b2.exception import CommandError
from .folder import B2Folder, LocalFolder


def parseSyncDir(dirPath, api):
    """
    Takes either a local path, or a B2 path, and returns a Folder object for it.

    B2 paths look like: b2://bucketName/path/name.
    Anything else is treated like a local folder.
    """
    if dirPath.startswith('b2://'):
        return __parseBucketAndFolder(dirPath[5:], api)
    else:
        return LocalFolder(dirPath)


def __parseBucketAndFolder(bucket_and_path, api):
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
