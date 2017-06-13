######################################################################
#
# File: sync/file.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
# Modified by: Alex Simovic
#
######################################################################
from functools import total_ordering

@total_ordering
class PathEntity(object):
    """
    Holds information about one file or dir.

    The name is relative to a folder in all cases.

    Files that have multiple versions (which only happens
    in B2, not in local folders) include information about
    all of the versions, most recent first.
    """

    def __init__(self, path, relativePath, isDir, versions):
        self.path = path
        self.relativePath = relativePath
        self.isDir = isDir
        self.versions = versions

    def latest_version(self):
        return self.versions[0]

    def __eq__(self, other):
        return self.isDir == other.isDir and \
               self.path.lower() == other.path.lower()

    def __le__(self, other):
        return self.path.lower() < other.path.lower()

    def __repr__(self):
        return 'File(%s, [%s])' % (self.name, ', '.join(repr(v) for v in self.versions))


class FileVersion(object):
    """
    Holds information about one version of a file:

       id - The B2 file id, or the local full path name, or compound id for index
       mod_time - modification time, in milliseconds, to avoid rounding issues
                  with millisecond times from B2
       action - "hide" or "upload" (never "start")
    """

    def __init__(self, id_, size, mod_time, md5):
        self.id_ = id_
        self.size = size
        self.mod_time = mod_time
        self.md5 = md5

    def __repr__(self):
        return 'FileVersion(%s, %s, %s, %s)' % (
            repr(self.id_), repr(self.mod_time)
        )
