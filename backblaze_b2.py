import os

import logging
from b2.account_info.sqlite_account_info import (SqliteAccountInfo)
from b2.api import (B2Api, B2RawApi)
from b2.api import Bucket
from b2.b2http import (B2Http)
from b2.cache import (AuthInfoCache)
from b2.download_dest import DownloadDestLocalFile
from b2.exception import B2Error
from b2.raw_api import SRC_LAST_MODIFIED_MILLIS
from b2.upload_source import UploadSourceLocalFile

import security
from utility import util

log = logging.getLogger()

def authorizeAccount(api, accountId, applicationKey):
    try:
        api.authorize_account('production', accountId, applicationKey)
        return 0
    except B2Error as e:
        print('ERROR: unable to authorize account: ' + str(e))
        return 1

def setupApi(conf):
    info = SqliteAccountInfo('b2_account_info')
    b2Http = B2Http()
    rawApi = B2RawApi(b2Http)
    b2Api = B2Api(info, AuthInfoCache(info), raw_api=rawApi)
    authorizeAccount(b2Api, conf.AccountId, conf.ApplicationKey)

    return b2Api

def getFileInfoByName(api, bucketName, fileName):
    bucket = api.get_bucket_by_name(bucketName)
    bucketFiles = bucket.list_file_names(fileName, 1)
    if not bucketFiles['files']:
        return None
    else:
        return bucketFiles['files'][0]

def getModTimeFromFileInfo(fileInfo):
    """
    get the mod time from the file info object for a remote file
    :param fileInfo: file info dictionary returned from api
    :return: mod time or None
    """
    # the fileInfo object should have a 'fileInfo' element which contains the custom info uploaded with the file
    if fileInfo is not None and 'fileInfo' in fileInfo:
        fileInfoData = fileInfo['fileInfo']
        if SRC_LAST_MODIFIED_MILLIS in fileInfoData:
            return int(fileInfoData[SRC_LAST_MODIFIED_MILLIS])
    return None

def downloadSecureFile(conf, api: B2Api, fileId, destination):
    tempPath = destination + util.APPLICATION_EXT

    dest = DownloadDestLocalFile(tempPath)
    api.download_file_by_id(fileId, dest)
    security.decompressAndDecrypt(conf, tempPath, destination)
    util.silentRemove(tempPath)
    log.info(f"Downloaded secure file: '{fileId}' to '{destination}'")
    return 0

def uploadSecureFile(conf, bucket: Bucket, filepath, saveModTime=False, customName=None):
    name = customName if customName else filepath
    if saveModTime:
        fileInfo = {SRC_LAST_MODIFIED_MILLIS: str(util.getModTime(filepath))}
    else:
        fileInfo = None

    tempPath = security.compressAndEncrypt(conf, filepath)
    secureName = security.generateSecureName(name)

    uploadSource = UploadSourceLocalFile(tempPath)
    fileVersionInfo = bucket.upload(uploadSource, secureName, file_info=fileInfo)

    util.silentRemove(tempPath)
    log.info(f"Uploaded secure file: '{filepath}' to '{fileVersionInfo.id_}'")

    return fileVersionInfo
