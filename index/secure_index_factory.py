import os

from b2_ext.api import B2Api

import backblaze_b2
import security
import logging
from index.secure_index import SecureIndex
from utility import util
from utility.config import ConfigException

log = logging.getLogger()

class IndexFactoryException(Exception):
    def __init__(self, text):
        self.text = text
    def __str__(self):
        return repr(self.text)

class SecureIndexFactory:
    def __init__(self, conf, api: B2Api, bucket_name):
        self.conf = conf
        self.api = api
        self.bucket_name = bucket_name

    def __getName(self):
        return self.bucket_name + '\index'

    # Find, create or download a local index
    def createIndex(self, forceLocalIndex):
        # try and find local file
        if os.path.isdir(self.conf.IndexPath):
            raise ConfigException('IndexPath cannot be a directory')

        forceUpload = False
        if not self.conf.args.test:
            forceUpload = not self.__getLatestIndex(forceLocalIndex)
            log.info('Marking secure index for upload because remote is older or missing')

        return SecureIndex(self.conf.IndexPath, self, forceUpload=forceUpload)

    def __getLatestIndex(self, forceLocalIndex):
        localModTime = None
        if os.path.exists(self.conf.IndexPath):
            localModTime = util.getModTime(self.conf.IndexPath)
            log.info('Local secure index found')
        elif forceLocalIndex:
            raise IndexFactoryException('Local secure index not found with forceLocalIndex=True')
        else:
            log.info('Local secure index not found')

        indexName = security.generateSecureName(self.conf, self.__getName())

        #get index by name, by id is unreliable because there could be multiple indexes
        #from different applications and we want the latest one
        fileInfo = backblaze_b2.getFileInfoByName(self.api, self.bucket_name, indexName)
        remoteModTime = backblaze_b2.getModTimeFromFileInfo(fileInfo)
        fileId = None if fileInfo is None else fileInfo['fileId']

        if fileInfo and not remoteModTime:
            log.info('Remote secure index has no timestamp')

        # Download if the local index doesnt exist of if its older
        # remoteModTime should always have a value if the file exists but it may have been improperly uploaded
        if not forceLocalIndex and remoteModTime and \
                (not localModTime or localModTime < remoteModTime):
            log.info('Downloading remote secure index because it newer')
            backblaze_b2.downloadSecureFile(conf=self.conf,
                                            api=self.api,
                                            fileId=fileId,
                                            destination=self.conf.IndexPath)

        # return if the remote index is up to date
        if remoteModTime and (not localModTime or localModTime <= remoteModTime):
            log.info('Local secure index is up to date')
            return True
        return False

    # Upload local index to b2
    def uploadIndex(self, secureIndex):
        if not secureIndex.hasChanges and not secureIndex.forceUpload:
            log.info('Index not changed, skipping upload')
            return
        if self.conf.args.test:
            return

        # cached by api
        bucket = self.api.get_bucket_by_name(self.bucket_name)
        backblaze_b2.uploadSecureFile(conf=self.conf,
                                      bucket=bucket,
                                      filepath=secureIndex.filename,
                                      saveModTime=True,
                                      customName=self.__getName())

        log.info('Index uploaded')
