import os

from b2.api import B2Api

import backblaze_b2
import security
from index.secure_index import SecureIndex
from utility import util
from utility.config import ConfigException


class SecureIndexFactory:
    def __init__(self, conf, api: B2Api, bucket_name):
        self.conf = conf
        self.api = api
        self.bucket_name = bucket_name

    def __getName(self):
        return self.bucket_name + '\index'

    # Find, create or download a local index
    def createIndex(self):
        # try and find local file
        if os.path.isdir(self.conf.IndexPath):
            raise ConfigException('IndexPath cannot be a directory')

        if not self.conf.args.test:
            self.__getLatestIndex()

        return SecureIndex(self.conf.IndexPath, self)

    def __getLatestIndex(self):
        localModTime = None
        if os.path.exists(self.conf.IndexPath):
            localModTime = util.getModTime(self.conf.IndexPath)

        indexName = security.generateSecureName(self.__getName())

        # try and get file info from b2
        fileInfo = None
        if self.conf.IndexFileId:
            try:
                fileInfo = self.api.get_file_info(self.conf.IndexFileId)
            except: # we have an id but the index doesn't exist in b2
                self.conf.IndexFileId = None

        if fileInfo:
            remoteModTime = backblaze_b2.getModTimeFromFileInfo(fileInfo)
            fileId = self.conf.IndexFileId
        else:
            fileInfo = backblaze_b2.getFileInfoByName(self.api, self.bucket_name, indexName)
            remoteModTime = backblaze_b2.getModTimeFromFileInfo(fileInfo)
            fileId = None if fileInfo is None else fileInfo['fileId']
            self.conf.IndexFileId = fileId

        if fileInfo and not remoteModTime:
            print('Remote secure index has not timestamp.')

        # Download if the local index doesnt exist of if its older
        # remoteModTime should always have a value if the file exists but it may have been improperly uploaded
        if remoteModTime and \
                (not localModTime or localModTime < remoteModTime):
            backblaze_b2.downloadSecureFile(conf=self.conf,
                                            api=self.api,
                                            fileId=fileId,
                                            destination=self.conf.IndexPath)

    # Upload local index to b2
    def uploadIndex(self, secureIndex):
        if not secureIndex.hasChanges:
            print('index not changed skipping upload')
            return

        # cached by api
        bucket = self.api.get_bucket_by_name(self.bucket_name)

        fi = backblaze_b2.uploadSecureFile(conf=self.conf,
                                           bucket=bucket,
                                           filepath=secureIndex.filename,
                                           saveModTime=True,
                                           customName=self.__getName())
        print('uploaded new index')
        self.conf.IndexFileId = fi.id_
