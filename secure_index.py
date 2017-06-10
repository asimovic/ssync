import util
import os
import backblaze_b2
from b2.api import B2Api

from config import ConfigException


class SecureIndex:
    def __init__(self, conf, api: B2Api, bucket_name):
        self.conf = conf
        self.api = api
        self.bucket_name = bucket_name

    def getIndex(self):
        #try and find local file
        if os.path.isdir(self.conf.IndexPath):
            raise ConfigException('IndexPath cannot be a directory')

        localModTime = None
        if os.path.exists(self.conf.IndexPath):
            localModTime = util.getModTime(self.conf.IndexPath)

        indexName = util.generateSecureName(self.bucket_name + '\index')

        #get file info from b2
        fileInfo = None
        if self.conf.IndexFileId:
            fileInfo = self.api.get_file_info(self.conf.IndexFileId)
            fileId = self.conf.IndexFileId
        else:
            fileInfo = backblaze_b2.getFileInfoByName(indexName)

        if not fileInfo:
            if not localModTime:
                #create
            else:
                #use local
        else:
            if not localModTime or localModTime < backblaze_b2.getModTimeFromFileInfo(fileInfo):
                #download new
            else:
                #use local

        return fileId
