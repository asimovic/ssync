######################################################################
#
# File: sync/sync.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
# Modified by: Alex Simovic
#
######################################################################

from __future__ import division

import logging
import re
import six
import threading

from b2.exception import CommandError
from .policy_manager import POLICY_MANAGER, SyncType
from .report import SyncReport

try:
    import concurrent.futures as futures
except ImportError:
    import futures

logger = logging.getLogger(__name__)


def __nextOrNone(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None


def __filter_folder(folder, reporter, exclusions, inclusions):
    """
    Filters a folder through a list of exclusions and inclusions.
    Inclusions override exclusions.
    """
    logging.debug('_filter_folder() exclusions for %s are %s', folder, exclusions)
    logging.debug('_filter_folder() inclusions for %s are %s', folder, inclusions)
    for f in folder.all_files(reporter):
        if any(pattern.match(f.name) for pattern in inclusions):
            logging.debug('_filter_folder() included %s from %s', f, folder)
            yield f
            continue
        if any(pattern.match(f.name) for pattern in exclusions):
            logging.debug('_filter_folder() excluded %s from %s', f, folder)
            continue
        yield f


def __iter_folders(folder_a, folder_b, reporter, exclusions=tuple(), inclusions=tuple()):
    """
    An iterator over all of the files in the union of two folders,
    matching file names.

    Each item is a pair (file_a, file_b) with the corresponding file
    in both folders.  Either file (but not both) will be None if the
    file is in only one folder.
    :param folder_a: A Folder object.
    :param folder_b: A Folder object.
    """

    iter_a = __filter_folder(folder_a, reporter, exclusions, inclusions)
    iter_b = folder_b.all_files(reporter)

    current_a = __nextOrNone(iter_a)
    current_b = __nextOrNone(iter_b)
    while current_a is not None or current_b is not None:
        if current_a is None:
            yield (None, current_b)
            current_b = __nextOrNone(iter_b)
        elif current_b is None:
            yield (current_a, None)
            current_a = __nextOrNone(iter_a)
        elif current_a.name < current_b.name:
            yield (current_a, None)
            current_a = __nextOrNone(iter_a)
        elif current_b.name < current_a.name:
            yield (None, current_b)
            current_b = __nextOrNone(iter_b)
        else:
            assert current_a.name == current_b.name
            yield (current_a, current_b)
            current_a = __nextOrNone(iter_a)
            current_b = __nextOrNone(iter_b)


def __make_file_sync_actions(sourceDir, source_file, destinationDir, dest_file,
                             syncType, now_millis, args):
    """
    Yields the sequence of actions needed to sync the two files
    """

    policy = POLICY_MANAGER.createPolicy(sourceDir, source_file, destinationDir, dest_file,
                                         syncType, now_millis, args)
    for action in policy.getAllActions():
        yield action


def __make_folder_sync_actions(sourceDir, destinationDir, args, now_millis, reporter):
    """
    Yields a sequence of actions that will sync the destination
    folder to the source folder.
    """
    if args.skipNewer and args.replaceNewer:
        raise CommandError('--skipNewer and --replaceNewer are incompatible')

    exclusions = [re.compile(ex) for ex in args.excludeRegex]
    inclusions = [re.compile(inc) for inc in args.includeRegex]

    if (sourceDir.type(), destinationDir.type()) not in [
        ('sec', 'local'), ('local', 'sec')
    ]:
        raise NotImplementedError("Sync support only local-to-b2 and b2-to-local")
    syncType = SyncType.UPLOAD if sourceDir.type() == 'local' else SyncType.DOWNLOAD

    for (source_file, dest_file) in \
            __iter_folders(sourceDir, destinationDir, reporter, exclusions, inclusions):
        if source_file is None:
            logging.debug('determined that %s is not present on source', dest_file)
        elif dest_file is None:
            logging.debug('determined that %s is not present on destination', source_file)

        if sourceDir.type() == 'local':
            if source_file is not None:
                reporter.update_compare(1)
        else:
            if dest_file is not None:
                reporter.update_compare(1)

        for action in __make_file_sync_actions(sourceDir, source_file, destinationDir, dest_file,
                                               syncType, now_millis, args):
            yield action


def count_files(local_folder, reporter):
    """
    Counts all of the files in a local folder.
    """
    # Don't pass in a reporter to all_files.  Broken symlinks will be reported
    # during the next pass when the source and dest files are compared.
    for _ in local_folder.all_files(None):
        reporter.update_local(1)
    reporter.end_local()


class BoundedQueueExecutor(object):
    """
    Wraps a futures.Executor and limits the number of requests that
    can be queued at once.  Requests to submit() tasks block until
    there is room in the queue.

    The number of available slots in the queue is tracked with a
    semaphore that is acquired before queueing an action, and
    released when an action finishes.
    """

    def __init__(self, executor, queue_limit):
        self.executor = executor
        self.semaphore = threading.Semaphore(queue_limit)

    def submit(self, fcn, *args, **kwargs):
        # Wait until there is room in the queue.
        self.semaphore.acquire()

        # Wrap the action in a function that will release
        # the semaphore after it runs.
        def run_it():
            try:
                fcn(*args, **kwargs)
            finally:
                self.semaphore.release()

        # Submit the wrapped action.
        return self.executor.submit(run_it)

    def shutdown(self):
        self.executor.shutdown()


def sync_folders(
    source_folder, dest_folder, args, now_millis, stdout, no_progress, max_workers, dry_run=False
):
    """
    Syncs two folders.  Always ensures that every file in the
    source is also in the destination.
    """

    # For downloads, make sure that the target directory is there.
    if dest_folder.type() == 'local' and not dry_run:
        dest_folder.ensure_present()

    # Make a reporter to report progress.
    with SyncReport(stdout, no_progress) as reporter:

        # Make an executor to count files and run all of the actions.  This is
        # not the same as the executor in the API object, which is used for
        # uploads.  The tasks in this executor wait for uploads.  Putting them
        # in the same thread pool could lead to deadlock.
        #
        # We use an executor with a bounded queue to avoid using up lots of memory
        # when syncing lots of files.
        unbounded_executor = futures.ThreadPoolExecutor(max_workers=max_workers)
        queue_limit = max_workers + 1000
        sync_executor = BoundedQueueExecutor(unbounded_executor, queue_limit=queue_limit)

        # First, start the thread that counts the local files.  That's the operation
        # that should be fastest, and it provides scale for the progress reporting.
        localFolder = None
        if source_folder.type() == 'local':
            localFolder = source_folder
        if dest_folder.type() == 'local':
            localFolder = dest_folder
        if localFolder is None:
            raise ValueError('neither folder is a local folder')
        sync_executor.submit(count_files, localFolder, reporter)

        # Schedule each of the actions
        remoteFolder = None
        if source_folder.type() == 'sec':
            remoteFolder = source_folder
        if dest_folder.type() == 'sec':
            remoteFolder = dest_folder
        if remoteFolder is None:
            raise ValueError('neither folder is a b2 folder')

        action_futures = []
        total_files = 0
        total_bytes = 0
        for action in __make_folder_sync_actions(source_folder, dest_folder, args, now_millis, reporter):
            logging.debug('scheduling action %s on bucket %s')
            future = sync_executor.submit(action.run, bucket, reporter, dry_run)
            action_futures.append(future)
            total_files += 1
            total_bytes += action.get_bytes()
        reporter.end_compare(total_files, total_bytes)

        # Wait for everything to finish
        sync_executor.shutdown()
        if any(1 for f in action_futures if f.exception() is not None):
            raise CommandError('sync is incomplete')
