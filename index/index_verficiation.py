from index.secure_index import SecureIndex
import logging

log = logging.getLogger()

class VerifyFile:
    def __init__(self, remoteId, remoteName, size):
        self.size = size
        self.remoteId = remoteId
        self.remoteName = remoteName

def ValidateAndUpdateIndex(bucket, folderName, secIndex: SecureIndex):
    """
    Ensure that the bucket and the index match, name only
    Updates index to match the bucket
    """

    indexFiles = {}
    for f in secIndex.getAll():
        if f.remoteName is not None:
            indexFiles[f.remoteName] = f

    log.info(f'Found ({len(indexFiles)}) files in index')

    #remove all item that are in the bucket
    for f in __iterateBucket(bucket, folderName):
        if f.remoteName in indexFiles:
            indexFile = indexFiles[f.remoteName]
            if indexFile.remoteId == f.remoteId:
                del indexFiles[f.remoteName]

    log.info(f'Removing ({len(indexFiles)}) files in that are no longer on the remote dir')
    for f in indexFiles:
        path = indexFiles[f].path
        log.info(f"Removing: '{path}' ({f})")
        secIndex.remove(path)
    secIndex.flush()


def __iterateBucket(bucket, folderName):
    folderName = '' if folderName == '' else folderName + '/'
    current_file = None

    for (file_version_info, file_folder_name) in bucket.ls(
            folderName, show_versions=True, recursive=True, fetch_count=1000
    ):
        assert file_version_info.file_name.startswith(folderName)
        if file_version_info.action == 'start':
            continue

        # ignore multiple file versions and just take latest
        file_name = file_version_info.file_name[len(folderName):]
        if current_file is None or current_file.remoteName != file_name:
            current_file = VerifyFile(file_version_info.id_, file_name, file_version_info.size)
            yield current_file
