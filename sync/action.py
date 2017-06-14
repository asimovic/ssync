######################################################################
#
# File: sync/action.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
# Modified by: Alex Simovic
#
######################################################################

from abc import (ABCMeta, abstractmethod)

import logging
import os
import six

from b2.download_dest import DownloadDestLocalFile
from b2.upload_source import UploadSourceLocalFile
from b2.utils import raise_if_shutting_down
from b2.raw_api import SRC_LAST_MODIFIED_MILLIS

import backblaze_b2
import util
from index.secure_index import IndexEntry
from .report import SyncFileReporter

logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class AbstractAction(object):
    """
    An action to take, such as uploading, downloading, or deleting
    a file.  Multi-threaded tasks create a sequence of Actions, which
    are then run by a pool of threads.

    An action can depend on other actions completing.  An example of
    this is making sure a CreateBucketAction happens before an
    UploadFileAction.
    """

    def run(self, remoteFolder, conf, reporter, dry_run=False):
        raise_if_shutting_down()
        try:
            if not dry_run:
                self.do_action(remoteFolder, conf, reporter)
            self.do_report(reporter)
        except Exception as e:
            logger.exception('an exception occurred in a sync action')
            reporter.error(str(self) + ": " + repr(e) + ' ' + str(e))
            raise  # Re-throw so we can identify failed actions

    @abstractmethod
    def get_bytes(self):
        """
        Returns the number of bytes to transfer for this action.
        """

    @abstractmethod
    def do_action(self, remoteFolder, conf, reporter):
        """
        Performs the action, returning only after the action is completed.
        """

    @abstractmethod
    def do_report(self, reporter):
        """
        Report the action performed.
        """


class B2UploadAction(AbstractAction):
    def __init__(self, sourceFile):
        self.sourceFile = sourceFile

    def get_bytes(self):
        return self.sourceFile.latest_version().size

    def do_action(self, remoteFolder, conf, reporter):
        sf = self.sourceFile
        if not sf.isDir:
            b2Name = util.generateSecureName(sf.relativeName)
            tempPath = util.compressAndEncrypt(conf, sf.nativePath)

            info = remoteFolder.bucket.upload(
                UploadSourceLocalFile(tempPath),
                b2Name,
                progress_listener=SyncFileReporter(reporter)
            )
            b2Id = info.id_
            b2Name = info.file_name
        else:
            b2Id = None
            b2Name = None

        ent = IndexEntry(sf.relativeName, sf.isDir, sf.latest_version().size,
                         sf.latest_version().mod_time, sf.latest_version().hash,
                         b2Id, b2Name)
        remoteFolder.secureIndex.add(ent)

    def do_report(self, reporter):
        reporter.print_completion('upload ' + self.sourceFile.relativeName)

    def __str__(self):
        return 'b2_upload: ' + self.sourceFile.relativeName


class B2DownloadAction(AbstractAction):
    def __init__(self, remoteFile, localPath):
        self.remoteFile = remoteFile
        self.localPath = localPath

    def get_bytes(self):
        return self.remoteFile.size

    def do_action(self, remoteFolder, conf, reporter):
        parentDir = os.path.dirname(self.localPath)
        util.checkDirectory(parentDir)

        if self.remoteFile.isDir:
            util.silentRemove(self.localPath)
            util.checkDirectory(self.localPath)
        else:
            # Download the file to a .tmp file
            downloadPath = self.localPath + '.b2.sync.tmp'
            destination = DownloadDestLocalFile(downloadPath)
            remoteFolder.bucket.download_file_by_name(
                self.remoteFile.nativePath, destination, SyncFileReporter(reporter))

            util.silentRemove(self.localPath)
            util.uncompressAndDecrypt(conf, downloadPath, self.localPath)

        modTime = self.remoteFile.mod_time / 1000.0
        os.utime(self.localPath, (modTime, modTime))

    def do_report(self, reporter):
        reporter.print_completion('download to ' + self.localPath)

    def __str__(self):
        return f'b2_download: f={self.remoteFile}, lp={self.localPath}'


class B2DeleteAction(AbstractAction):
    def __init__(self, remoteFile):
        self.remoteFile = remoteFile

    def get_bytes(self):
        return 0

    def do_action(self, remoteFolder, conf, reporter):
        if not self.remoteFile.isDir:
            remoteFolder.bucket.api.delete_file_version(self.remoteFile.remoteId, self.remoteFile.remoteName)
        remoteFolder.secureIndex.remove(self.remoteFile.relativePath)

    def do_report(self, reporter):
        reporter.update_transfer(1, 0)
        reporter.print_completion('delete ' + self.remoteFile.relativePath)

    def __str__(self):
        return 'b2_delete: ' + self.remoteFile.relativePath


class LocalDeleteAction(AbstractAction):
    def __init__(self, path):
        self.path = path

    def get_bytes(self):
        return 0

    def do_action(self, destinationDir, conf, reporter):
        util.silentRemove(self.path)

    def do_report(self, reporter):
        reporter.update_transfer(1, 0)
        reporter.print_completion('delete ' + self.path)

    def __str__(self):
        return 'local_delete: ' + self.path
