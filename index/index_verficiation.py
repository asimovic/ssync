from index.secure_index import SecureIndex

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
        indexFiles[f.remoteName] = f

    #remove all item that are in the bucket
    for f in __iterateBucket(bucket, folderName):
        if f.remoteName in indexFiles:
            indexFile = indexFiles[f.remoteName]
            if (indexFile.remoteId == f.remoteId and
                indexFile.size == f.size):
                del indexFiles[f.remoteName]

    for f in indexFiles:
        secIndex.remove(f)
    secIndex.flush()


def __iterateBucket(bucket, folderName):
    folderName = '' if folderName == '' else folderName + '/'
    current_file = None

    for (file_version_info, file_folder_name) in bucket.list_file_names(
            folderName, show_versions=True, recursive=True, fetch_count=1000
    ):
        assert file_version_info.file_name.startswith(folderName)
        if file_version_info.action == 'start':
            continue

        # ignore multiple file versions and just take latest
        file_name = file_version_info.file_name[len(folderName):]
        if current_file.remoteName != file_name and current_file is not None:
            yield current_file
        else:
            current_file = VerifyFile(file_version_info.id_, file_name, file_version_info.size)
    if current_file is not None:
        yield current_file