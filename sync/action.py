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

    def run(self, bucket, reporter, dry_run=False):
        raise_if_shutting_down()
        try:
            if not dry_run:
                self.do_action(bucket, reporter)
            self.do_report(bucket, reporter)
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
    def do_action(self, bucket, reporter):
        """
        Performs the action, returning only after the action is completed.
        """

    @abstractmethod
    def do_report(self, bucket, reporter):
        """
        Report the action performed.
        """


class B2UploadAction(AbstractAction):
    def __init__(self, localPath, relativeName, size):
        self.localPath = localPath
        self.relativeName = relativeName
        self.size = size

    def get_bytes(self):
        return self.size

    def do_action(self, bucket, reporter):
        b2Name = util.generateSecureName(self.relativeName)

        bucket.upload(
            UploadSourceLocalFile(self.localPath),
            b2Name,
            progress_listener=SyncFileReporter(reporter)
        )

    def do_report(self, bucket, reporter):
        reporter.print_completion('upload from ' + self.localPath)

    def __str__(self):
        return 'b2_upload: ' + self.localPath


class B2DownloadAction(AbstractAction):
    def __init__(self, b2Name, b2Id, localPath, size):
        self.b2Name = b2Name
        self.b2Id = b2Id
        self.localPath = localPath
        self.size = size

    def get_bytes(self):
        return self.size

    def do_action(self, conf, bucket, reporter):
        # Make sure the directory exists
        parent_dir = os.path.dirname(self.localPath)
        if not os.path.isdir(parent_dir):
            try:
                os.makedirs(parent_dir)
            except OSError:
                pass
        if not os.path.isdir(parent_dir):
            raise Exception('could not create directory %s' % (parent_dir,))

        # Download the file to a .tmp file
        downloadPath = self.localPath + '.b2.sync.tmp'
        destination = DownloadDestLocalFile(downloadPath)
        bucket.download_file_by_name(self.b2Name, destination, SyncFileReporter(reporter))

        # Move the file into place
        try:
            util.silentRemove(self.localPath)
        except OSError:
            pass
        util.uncompressAndDecrypt(conf, downloadPath, self.localPath)

    def do_report(self, bucket, reporter):
        reporter.print_completion('download to ' + self.localPath)

    def __str__(self):
        return (
            'b2_download(%s, %s, %s, %d)' %
            (self.b2Name, self.b2Id, self.localPath, self.size)
        )


class B2HideAction(AbstractAction):
    def __init__(self, relative_name, b2_file_name):
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name

    def get_bytes(self):
        return 0

    def do_action(self, bucket, reporter):
        bucket.hide_file(self.b2_file_name)

    def do_report(self, bucket, reporter):
        reporter.update_transfer(1, 0)
        reporter.print_completion('hide   ' + self.relative_name)

    def __str__(self):
        return 'b2_hide(%s)' % (self.b2_file_name,)


class B2DeleteAction(AbstractAction):
    def __init__(self, displayName, b2Name, b2Id):
        self.displayName = displayName
        self.b2Name = b2Name
        self.b2Id = b2Id

    def get_bytes(self):
        return 0

    def do_action(self, bucket, reporter):
        bucket.api.delete_file_version(self.b2Id, self.b2Name)

    def do_report(self, bucket, reporter):
        reporter.update_transfer(1, 0)
        reporter.print_completion('delete ' + self.displayName)

    def __str__(self):
        return 'b2_delete: ' + self.displayName


class LocalDeleteAction(AbstractAction):
    def __init__(self, path):
        self.path = path

    def get_bytes(self):
        return 0

    def do_action(self, bucket, reporter):
        util.silentRemove(self.path)

    def do_report(self, bucket, reporter):
        reporter.update_transfer(1, 0)
        reporter.print_completion('delete ' + self.path)

    def __str__(self):
        return 'local_delete: ' + self.path
