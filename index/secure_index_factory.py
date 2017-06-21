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

        return SecureIndex(self.conf.IndexPath)

    def __getLatestIndex(self):
        localModTime = None
        if os.path.exists(self.conf.IndexPath):
            localModTime = util.getModTime(self.conf.IndexPath)

        indexName = security.generateSecureName(self.__getName())

        # get file info from b2
        if self.conf.IndexFileId:
            fileInfo = self.api.get_file_info(self.conf.IndexFileId)
            fileId = self.conf.IndexFileId  # TODO: fake implementation
        else:
            fileInfo = backblaze_b2.getFileInfoByName(self.api, self.bucket_name, indexName)
            fileId = fileInfo.id_  # TODO: fake implementation
            self.conf.IndexFileId = fileId

        # Download if the local index doesnt exist of if its older
        if fileInfo and (not localModTime or localModTime < backblaze_b2.getModTimeFromFileInfo(
                fileInfo)):  # TODO: fake implementation
            backblaze_b2.downloadSecureFile(self.api, fileId, self.conf.IndexPath)

    # Upload local index to b2
    def storeIndex(self):
        #todo: update file id somehow
        backblaze_b2.uploadSecureFile(self.api,
                                      self.bucket_name,
                                      self.conf.IndexPath,
                                      saveModTime=True,
                                      customName=self.__getName())

