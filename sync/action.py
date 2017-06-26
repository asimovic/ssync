import os
import security
import six
import logging

from abc import (ABCMeta, abstractmethod)
from b2.download_dest import DownloadDestLocalFile
from b2.upload_source import UploadSourceLocalFile
from b2.utils import raise_if_shutting_down

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
            log.info('Running action: ' + str(self))
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
        b2Id = None
        b2Name = None

        if not sf.isDir:
            b2Name = security.generateSecureName(sf.relativePath)

            if not conf.args.test:
                getHash = sf.latest_version().hash is None
                tempPath, hashDigest = security.compressAndEncryptWithHash(conf, sf.nativePath, getHash)
                if getHash:
                    sf.latest_version().hash = hashDigest

                info = remoteFolder.bucket.upload(
                    UploadSourceLocalFile(tempPath),
                    b2Name,
                    progress_listener=SyncFileReporter(reporter)
                )
                b2Id = info.id_
                b2Name = info.file_name

                # delete the temp file after the upload
                util.silentRemove(tempPath)

        ent = IndexEntry(path=sf.relativePath,
                         isDir=sf.isDir,
                         size=sf.latest_version().size,
                         modTime=sf.latest_version().mod_time,
                         hash=sf.latest_version().hash,
                         remoteId=b2Id,
                         remoteName=b2Name)
        remoteFolder.secureIndex.add(ent)

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
            remoteFolder.bucket.api.delete_file_version(
                self.remoteFile.latest_version().id_,
                self.remoteFile.nativePath)
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
