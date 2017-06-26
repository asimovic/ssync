from abc import ABCMeta, abstractmethod
from b2_ext.exception import CommandError
from utility import util
from .action import LocalDeleteAction, B2DeleteAction, B2DownloadAction, B2UploadAction

ONE_DAY_IN_MS = 24 * 60 * 60 * 1000

class AbstractFileSyncPolicy(metaclass=ABCMeta):
    DESTINATION_PREFIX = NotImplemented
    SOURCE_PREFIX = NotImplemented

    def __init__(self, sourceDir, source_file, destinationDir, dest_file, now_millis, args):
        self.sourceDir = sourceDir
        self.sourceFile = source_file
        self.destinationDir = destinationDir
        self.destinationFile = dest_file
        self.comparison = args.comparison
        self.nowMillis = now_millis

    def __should_transfer(self):
        """
        Decides whether to transfer the file from the source to the destination.
        """
        if self.sourceFile is None:
            # No source file.  Nothing to transfer.
            return False
        elif self.destinationFile is None:
            # Source file exists, but no destination file.  Always transfer.
            return True
        else:
            # Both exist.  Transfer only if the two are different.
            return self.__files_are_different(self.sourceDir, self.sourceFile,
                                              self.destinationDir, self.destinationFile,
                                              self.comparison)

    @classmethod
    def __files_are_different(cls, sourceDir, sourceFile,
                              destinationDir, destinationFile,
                              comparison):
        # Compare two files and determine if the the destination file should be replaced by the source file.
        if not comparison:
            comparison = '4'
        if not util.is_int(comparison):
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
            yield self._make_transfer_action()

        assert self.destinationFile is not None or self.sourceFile is not None

        for action in self._getDeleteActions():
            yield action

    def _getDeleteActions(self):
        """ subclass policy can override this to hide or delete files """
        return []

    def _get_source_mod_time(self):
        return self.sourceFile.latest_version().mod_time

    @abstractmethod
    def _make_transfer_action(self):
        """ return an action representing transfer of file according to the selected policy """


class UpPolicy(AbstractFileSyncPolicy):
    # File is synced up (from disk the cloud)
    DESTINATION_PREFIX = 'b2://'
    SOURCE_PREFIX = 'local://'

    def shouldDeleteOld(self):
        return False

    def _make_transfer_action(self):
        upload = B2UploadAction(self.sourceFile)
        if self.shouldDeleteOld() and self.destinationFile is not None:
            delete = B2DeleteAction(self.destinationFile)
            return delete, upload
        return upload


class DownPolicy(AbstractFileSyncPolicy):
    # File is synced down (from the cloud to disk)
    DESTINATION_PREFIX = 'local://'
    SOURCE_PREFIX = 'b2://'

    def _make_transfer_action(self):
        return B2DownloadAction(
            self.sourceFile,
            self.destinationDir.getFullPathForSubFile(self.sourceFile)
        )


class UpAndDeletePolicy(UpPolicy):
    def shouldDeleteOld(self):
        return True

    # File is synced up (from disk to the cloud) and the delete flag is SET
    def _getDeleteActions(self):
        # if the destination exits then it only has 1 version since the secure index only supports 1
        if self.destinationFile is not None and self.sourceFile is None:
            yield B2DeleteAction(self.destinationFile)


class DownAndDeletePolicy(DownPolicy):
    # File is synced down (from the cloud to disk) and the delete flag is SET
    def _getDeleteActions(self):
        if self.destinationFile is not None and self.sourceFile is None:
            yield LocalDeleteAction(self.destinationFile.nativePath)
