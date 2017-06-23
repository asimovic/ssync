from functools import total_ordering

@total_ordering
class PathEntity(object):
    """
    Holds information about one file or dir.
    Files that have multiple versions (which only happens in B2, not in local folders) include information about
    all of the versions, most recent first.

    :param nativePath: path to the file that can be used for access. Full local path or encrypted b2 path
    :param relativePath: relative path to folder that is normalized and can be used for comparison
    :param isDir: bool indicating if the path is a directory
    """

    def __init__(self, nativePath, relativePath, isDir, versions):
        self.nativePath = nativePath
        self.relativePath = relativePath
        self.isDir = isDir
        self.versions = versions

    def latest_version(self):
        return self.versions[0]

    def __eq__(self, other):
        if other is None:
            return False
        return self.isDir == other.isDir and \
               self.relativePath.lower() == other.relativePath.lower()

    def __le__(self, other):
        return self.relativePath.lower() < other.relativePath.lower()

    def __repr__(self):
        return 'File: ' + self.relativePath


class FileVersion(object):
    """
    Holds information about one version of a file:

       id - The B2 file id, or the local full path name, or compound id for index
       mod_time - modification time, in milliseconds, to avoid rounding issues
                  with millisecond times from B2
       action - "hide" or "upload" (never "start")
    """

    def __init__(self, id_, size, mod_time, hash):
        self.id_ = id_
        self.size = size
        self.mod_time = mod_time
        self.hash = hash

    def __repr__(self):
        return 'FileVersion(%s, %s)' % (
            repr(self.id_), repr(self.mod_time)
        )
