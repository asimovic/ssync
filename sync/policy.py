######################################################################
#
# File: sync/policy.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
# Modified by: Alex Simovic
#
######################################################################

from abc import ABCMeta, abstractmethod

from b2.exception import CommandError
from .action import LocalDeleteAction, B2DeleteAction, B2DownloadAction,  B2UploadAction

ONE_DAY_IN_MS = 24 * 60 * 60 * 1000

class AbstractFileSyncPolicy(metaclass=ABCMeta):
    DESTINATION_PREFIX = NotImplemented
    SOURCE_PREFIX = NotImplemented

    def __init__(self, sourceDir, source_file, destinationDir, dest_file, now_millis, args):
        self.__sourceDir = sourceDir
        self.__sourceFile = source_file
        self.__destinationDir = destinationDir
        self.__destinationFile = dest_file
        self.__delete = args.delete
        self.__comparison = args.comparison
        self.__nowMillis = now_millis
        self.__transferred = False

    def __should_transfer(self):
        """
        Decides whether to transfer the file from the source to the destination.
        """
        if self.__sourceFile is None:
            # No source file.  Nothing to transfer.
            return False
        elif self.__destinationFile is None:
            # Source file exists, but no destination file.  Always transfer.
            return True
        else:
            # Both exist.  Transfer only if the two are different.
            return self.__files_are_different(self.__sourceDir, self.__sourceFile,
                                              self.__destinationDir, self.__destinationFile,
                                              self.__comparison)

    @classmethod
    def __files_are_different(cls, sourceDir, sourceFile,
                              destinationDir, destinationFile,
                              comparison):
        # Compare two files and determine if the the destination file should be replaced by the source file.
        if not comparison:
            comparison = '4'
        if not comparison.isdigit():
            raise CommandError('Invalid option for --compareVersions')

        compareLevel = int(comparison)
        areDifferent = False

        # Compare using file name only
        if compareLevel >= 1 and not areDifferent:
            areDifferent = sourceFile.isDir != destinationFile.isDir

        # Remaining comparisons can't be done on directories
        if sourceFile.isDir or destinationFile.isDir:
            return areDifferent

        # Compare using file size
        if compareLevel >= 2 and not areDifferent:
            s1 = sourceFile.latest_version().size
            s2 = destinationFile.latest_version().size
            areDifferent = s1 != s2

        # Compare using modification time
        if compareLevel >= 3 and not areDifferent:
            sTime = sourceFile.latest_version().mod_time
            dTime = destinationFile.latest_version().mod_time
            # We don't care which one is newer, they are different and the source is the master
            areDifferent = sTime != dTime

        if compareLevel >= 4 and not areDifferent:
            h1 = sourceDir.updateHashForSubFile(sourceFile)
            h2 = destinationDir.updateHashForSubFile(destinationFile)
            # If we can't get a hash for a file then ignore the check
            areDifferent = h1 is not None and h2 is not None and h1 != h2

        return areDifferent

    def getAllActions(self):
        if self.__should_transfer():
            yield self.__make_transfer_action()
            self.__transferred = True

        assert self.__destinationFile is not None or self.__sourceFile is not None

        for action in self._getHideOrDeleteActions():
            yield action

    def _getHideOrDeleteActions(self):
        """ subclass policy can override this to hide or delete files """
        return []

    def _get_source_mod_time(self):
        return self.__sourceFile.latest_version().mod_time

    @abstractmethod
    def __make_transfer_action(self):
        """ return an action representing transfer of file according to the selected policy """


class UpPolicy(AbstractFileSyncPolicy):
    # File is synced up (from disk the cloud)
    DESTINATION_PREFIX = 'b2://'
    SOURCE_PREFIX = 'local://'

    def __make_transfer_action(self):
        return B2UploadAction(
            self.__sourceFile
        )


class DownPolicy(AbstractFileSyncPolicy):
    # File is synced down (from the cloud to disk)
    DESTINATION_PREFIX = 'local://'
    SOURCE_PREFIX = 'b2://'

    def __make_transfer_action(self):
        return B2DownloadAction(
            self.__sourceFile,
            self.__destinationDir.getFullPathForSubFile(self.__sourceFile.relativePath)
        )


class UpAndDeletePolicy(UpPolicy):
    # File is synced up (from disk to the cloud) and the delete flag is SET
    def _getHideOrDeleteActions(self):
        for action in super(UpAndDeletePolicy, self)._getHideOrDeleteActions():
            yield action
        for action in makeB2DeleteActions(self.__sourceFile, self.__destinationFile, self.__transferred):
            yield action


class DownAndDeletePolicy(DownPolicy):
    # File is synced down (from the cloud to disk) and the delete flag is SET
    def _getHideOrDeleteActions(self):
        for action in super(DownAndDeletePolicy, self)._getHideOrDeleteActions():
            yield action
        if self.__destinationFile is not None and self.__sourceFile is None:
            # Local files have either 0 or 1 versions.  If the file is there,
            # it must have exactly 1 version.
            yield LocalDeleteAction(self.__destinationFile.nativePath)


def makeB2DeleteActions(sourceFile, destinationFile, transferred):
    """
    Creates the actions to delete files stored on B2, which are not present locally.
    """
    for version_index, version in enumerate(destinationFile.versions):
        keep = (version_index == 0) and (sourceFile is not None) and not transferred
        if not keep:
            yield B2DeleteAction(
                destinationFile
            )