######################################################################
#
# File: sync/folder.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
# Modified by: Alex Simovic
#
######################################################################

import os
import sys
from abc import ABCMeta, abstractmethod

from utility import util
from .exception import EnvironmentEncodingError
from .path_entity import PathEntity, FileVersion


class AbstractFolder(metaclass=ABCMeta):
    """
    Interface to a folder full of files, which might be a B2 bucket,
    a virtual folder in a B2 bucket, or a directory on a local file
    system.

    Files in B2 may have multiple versions, while files in local
    folders have just one.
    """

    @abstractmethod
    def all_files(self, reporter):
        """
        Returns an iterator over all of the files in the folder, in
        the order that B2 uses.

        No matter what the folder separator on the local file system
        is, "/" is used in the returned file names.

        If a file is found, but does not exist (for example due to
        a broken symlink or a race), reporter will be informed about
        each such problem.
        """

    @abstractmethod
    def type(self):
        """
        Returns one of:  'b2', 'local'
        """

    @abstractmethod
    def updateHashForSubFile(self, fileEntity):
        """
        Try and update the md5 hash of the file, file should belong to this folder
        :param fileEntity:
        :return:
        """

    @abstractmethod
    def getFullPathForSubFile(self, fileEntity):
        """
        Only for local folders, returns the full path to the file.
        """


class LocalFolder(AbstractFolder):
    """
    Folder interface to a directory on the local machine.
    """

    def __init__(self, path):
        """
        Initializes a new folder.
        :param path: Path to the root of the local folder.  Must be unicode.
        """
        if not isinstance(path, str):
            raise ValueError('folder path should be unicode: %s' % repr(path))
        self.path = os.path.abspath(path)
        if not self.path.endswith(os.sep):
            self.path += os.sep

    def type(self):
        return 'local'

    def all_files(self, reporter):
        for (full_path, isDir) in self.__walk_relative_paths(self.path, reporter):
            try:
                yield self.__makePathEntity(full_path, isDir)
            except:
                print('Failed to create path entity: ' + full_path)

    def getFullPathForSubFile(self, fileEntity):
        return os.path.join(self.path, fileEntity.relativePath.replace('/', os.path.sep))

    def ensure_present(self):
        """
        Makes sure that the directory exists.
        """
        if not os.path.exists(self.path):
            try:
                os.mkdir(self.path)
            except:
                raise Exception('unable to create directory %s' % (self.path,))
        elif not os.path.isdir(self.path):
            raise Exception('%s is not a directory' % (self.path,))

    def __walk_relative_paths(self, dir_path, reporter):
        """
        Yields all of the file names anywhere under this folder, in the
        order they would appear in B2. String sorting order.
        """
        if not isinstance(dir_path, str):
            raise ValueError('folder path should be unicode: %s' % repr(dir_path))

        # Collect the names
        # We know the dir_path is unicode, which will cause os.listdir() to
        # return unicode paths.
        names = []
        paths = []
        try:
            paths = os.listdir(dir_path)
        except Exception:
            print('Failed to get children of: ' + dir_path)

        for name in paths:
            # We expect listdir() to return unicode if dir_path is unicode.
            # If the file name is not valid, based on the file system
            # encoding, then listdir() will return un-decoded str/bytes.
            if not isinstance(name, str):
                name = self.__handle_non_unicode_file_name(name)

            full_path = os.path.join(dir_path, name)

            # Skip broken symlinks or other inaccessible files
            if not os.path.exists(full_path):
                if reporter is not None:
                    reporter.local_access_error(full_path)
            elif not os.access(full_path, os.R_OK):
                if reporter is not None:
                    reporter.local_permission_error(full_path)
            else:
                isDir = os.path.isdir(full_path)
                if isDir:
                    full_path += os.sep
                # need to keep sorting consistent and different path separators can change sorting between folder types
                sortPath = full_path.replace(os.sep, '/')
                names.append((full_path, sortPath, isDir))

        # Yield all of the answers
        for full_path, tmp, isDir in sorted(names, key=lambda x: x[1].lower()):
            if isDir:
                yield (full_path, True)
                for rp in self.__walk_relative_paths(full_path, reporter):
                    yield rp
            else:
                yield (full_path, False)

    def __handle_non_unicode_file_name(self, name):
        """
        Decide what to do with a name returned from os.listdir()
        that isn't unicode.  We think that this only happens when
        the file name can't be decoded using the file system
        encoding. Just in case that's not true, we'll allow all-ascii
        names.
        """
        # if it's all ascii, allow it
        if all(b <= 127 for b in name):
            return name
        raise EnvironmentEncodingError(repr(name), sys.getfilesystemencoding())

    def __makePathEntity(self, fullPath, isDir):
        relativePath = fullPath[len(self.path):]
        # Normalize path separators to match b2
        normalRelativePath = util.normalizePath(relativePath, isDir)
        mod_time = util.getModTime(fullPath)
        size = 0 if isDir else os.path.getsize(fullPath)

        # Hash is computed later
        version = FileVersion(id_=fullPath,
                              size=size,
                              mod_time=mod_time,
                              hash=None)
        return PathEntity(fullPath, normalRelativePath, isDir, [version])

    def updateHashForSubFile(self, pathEntity):
        if not pathEntity.latest_version().hash and not pathEntity.isDir:
            pathEntity.latest_version().hash = util.calculateHash(pathEntity.nativePath)
        return pathEntity.latest_version().hash

    def __repr__(self):
        return 'LocalFolder: ' + self.path


class SecureFolder(AbstractFolder):
    """
    Folder interface using the secureIndex.

    :param path: relative path to folder. Root should be the same path as the index root.
    :param secureIndex: secure index containing normalized path names
    """

    def __init__(self, path, secureIndex, bucket):
        self.path = util.normalizePath(path, True)
        self.secureIndex = secureIndex
        self.bucket = bucket

    def all_files(self, reporter):
        for fileInfo in self.secureIndex.getAll():
            if self.path != '':
                # Index is sorted by name so try and find the dir and start from there
                if fileInfo < self.path:
                    continue
                # We either passed all of the files in this folder or there were none
                if not fileInfo.path.startswith(self.path.lower()):
                    break;

            version = FileVersion(id_=fileInfo.remoteId,
                                  size=fileInfo.size,
                                  mod_time=fileInfo.modTime,
                                  hash=fileInfo.hash)
            pathEntity = PathEntity(fileInfo.remoteName, fileInfo.path, fileInfo.isDir, [version])

            yield pathEntity

    def type(self):
        return 'sec'

    def getFullPathForSubFile(self, fileEntity):
        if self.path == '':
            return fileEntity.relativePath
        else:
            return self.path + '/' + fileEntity.relativePath

    def updateHashForSubFile(self, pathEntity):
        # Try to get hash from the index, but it should always have one already
        if not pathEntity.latest_version().hash:
            indexPE = self.secureIndex.get(pathEntity.relativePath)
            if indexPE is not None:
                pathEntity.latest_version().hash = indexPE.hash
        return pathEntity.latest_version().hash

    def __str__(self):
        return 'SecFolder: ' + self.path


# class B2Folder(AbstractFolder):
#     """
#     Folder interface to B2.
#     """
#
#     def __init__(self, bucket_name, folder_name, api):
#         self.bucket_name = bucket_name
#         self.folder_name = folder_name
#         self.bucket = api.get_bucket_by_name(bucket_name)
#         self.prefix = '' if self.folder_name == '' else self.folder_name + '/'
#
#     def all_files(self, reporter):
#         current_name = None
#         current_versions = []
#         for (file_version_info, folder_name) in self.bucket.ls(
#             self.folder_name, show_versions=True, recursive=True, fetch_count=1000
#         ):
#             assert file_version_info.file_name.startswith(self.prefix)
#             if file_version_info.action == 'start':
#                 continue
#             file_name = file_version_info.file_name[len(self.prefix):]
#             if current_name != file_name and current_name is not None:
#                 yield PathEntity(current_name, False, current_versions)
#                 current_versions = []
#             file_info = file_version_info.file_info
#             if SRC_LAST_MODIFIED_MILLIS in file_info:
#                 mod_time_millis = int(file_info[SRC_LAST_MODIFIED_MILLIS])
#             else:
#                 mod_time_millis = file_version_info.upload_timestamp
#             assert file_version_info.size is not None
#             file_version = FileVersion(
#                 file_version_info.id_, file_version_info.file_name, mod_time_millis,
#                 file_version_info.action, file_version_info.size
#             )
#             current_versions.append(file_version)
#             current_name = file_name
#         if current_name is not None:
#             yield PathEntity(current_name, False, current_versions)
#
#     def folder_type(self):
#         return 'b2'
#
#     def make_full_path(self, file_name):
#         if self.folder_name == '':
#             return file_name
#         else:
#             return self.folder_name + '/' + file_name
#
#     def __str__(self):
#         return 'B2Folder(%s, %s)' % (self.bucket_name, self.folder_name)
