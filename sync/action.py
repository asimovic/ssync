import os

import b2_ext
import security
import six
import logging
import threading

from abc import (ABCMeta, abstractmethod)
from b2_ext.download_dest import DownloadDestLocalFile
from b2_ext.upload_source import UploadSourceLocalFile
from b2_ext.utils import raise_if_shutting_down

from index.secure_index import IndexEntry
from utility import util
from .report import SyncFileReporter

log = logging.getLogger()


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
            log.info(f'Starting action ({threading.get_ident()}): {str(self)}')
            if not dry_run:
                self.do_action(remoteFolder, conf, reporter)
            self.do_report(reporter)
        except Exception as e:
            log.exception('an exception occurred in a sync action')
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

        ent = IndexEntry(path=sf.relativePath,
                         isDir=sf.isDir,
                         size=sf.latest_version().size,
                         modTime=sf.latest_version().mod_time,
                         hash=None,
                         remoteId=None,
                         remoteName=None)

        if not sf.isDir:
            b2Name = security.generateSecureName(sf.relativePath)

            resume = False
            getHash = sf.latest_version().hash is None

            # check if we need to resume a large file upload
            if os.path.exists(security.getTempPath(sf.nativePath)):
                log.info('Found temp file for: ' + sf.relativePath)
                ie = remoteFolder.secureIndex.get(sf.relativePath)
                resume = ie and ie.status == 'uploading'
                if not resume:
                    log.info('No pending upload for file')

            tempPath = None
            if resume:
                log.info('Attempting to resume upload from temp file')
                sf.latest_version().hash = ie.hash
                #todo:add temp file validation
                log.info('Resuming previous upload')
                tempPath = security.getTempPath(sf.nativePath)

            if tempPath is None:
                tempPath, hashDigest = security.compressAndEncryptWithHash(conf, sf.nativePath, getHash)
                if getHash:
                    sf.latest_version().hash = hashDigest
            ent.hash = sf.latest_version().hash

            # write working status so we don't have to re-encrypt when resuming large files
            if self.sourceFile.latest_version().size > conf.largeFileBytes:
                ent.status = 'uploading'
                remoteFolder.secureIndex.addorUpdate(ent)

            try:
                if not conf.args.test:
                    info = remoteFolder.bucket.upload(
                        UploadSourceLocalFile(tempPath),
                        b2Name,
                        min_large_file_size=conf.largeFileBytes,
                        ignore_unfinished_check=not resume,
                        progress_listener=SyncFileReporter(reporter)
                    )
                    ent.remoteId = info.id_
                    ent.remoteName = info.file_name
            finally:
                # delete the temp file after the upload
                util.silentRemove(tempPath)

        ent.status = None
        remoteFolder.secureIndex.addorUpdate(ent)

    def do_report(self, reporter):
        text = 'Uploaded ' + self.sourceFile.relativePath
        reporter.print_completion(text)
        return text

    def __str__(self):
        return 'b2_upload: ' + self.sourceFile.relativePath


class B2DownloadAction(AbstractAction):
    def __init__(self, remoteFile, localPath):
        self.remoteFile = remoteFile
        self.localPath = localPath

    def get_bytes(self):
        return self.remoteFile.latest_version().size

    def do_action(self, remoteFolder, conf, reporter):
        parentDir = os.path.dirname(self.localPath)
        util.checkDirectory(parentDir)

        if self.remoteFile.isDir:
            util.checkDirectory(self.localPath)
        elif conf.args.test:
            util.silentRemove(self.localPath)
            open(self.localPath, 'a').close()
        else:
            # Download the file to a .tmp file
            downloadPath = self.localPath + '.b2.sync.tmp'
            destination = DownloadDestLocalFile(downloadPath)

            remoteFolder.bucket.download_file_by_name(
                self.remoteFile.nativePath, destination, SyncFileReporter(reporter))
            security.decompressAndDecrypt(conf, downloadPath, self.localPath)

            util.silentRemove(downloadPath)

        modTime = self.remoteFile.latest_version().mod_time / 1000.0
        os.utime(self.localPath, (modTime, modTime))

    def do_report(self, reporter):
        text = 'Downloaded ' + self.localPath
        reporter.print_completion(text)
        return text

    def __str__(self):
        return f'b2_download: f={self.remoteFile}, lp={self.localPath}'


class B2DeleteAction(AbstractAction):
    def __init__(self, remoteFile):
        self.remoteFile = remoteFile

    def get_bytes(self):
        return 0

    def do_action(self, remoteFolder, conf, reporter):
        if not self.remoteFile.isDir and not conf.args.test:
            try:
                remoteFolder.bucket.api.delete_file_version(
                    self.remoteFile.latest_version().id_,
                    self.remoteFile.nativePath)
            except b2_ext.exception.FileNotPresent:
                # ignore if the files doesn't exist, operation was likely interrupted and index is wrong
                pass
        remoteFolder.secureIndex.remove(self.remoteFile.relativePath)

    def do_report(self, reporter):
        reporter.update_transfer(1, 0)
        text = 'Deleted remote ' + self.remoteFile.relativePath
        reporter.print_completion(text)
        return text

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
        text = 'Deleted local ' + self.path
        reporter.print_completion(text)
        return text

    def __str__(self):
        return 'local_delete: ' + self.path
