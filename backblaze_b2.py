import os

from b2.raw_api import SRC_LAST_MODIFIED_MILLIS

import util
from b2.exception import B2Error
from b2.api import Bucket
from b2.upload_source import UploadSourceLocalFile
from b2.account_info.sqlite_account_info import (SqliteAccountInfo)
from b2.api import (B2Api, B2RawApi)
from b2.b2http import (B2Http)
from b2.cache import (AuthInfoCache)


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
    if SRC_LAST_MODIFIED_MILLIS in fileInfo:
        return int(fileInfo[SRC_LAST_MODIFIED_MILLIS])
    else:
        return fileInfo['uploadTimestamp']


def downloadSecureFile(conf, bucket: Bucket, fileId, destination):
    tempPath = os.path.join(conf.temp, os.path.basename(destination))
    util.silentRemove(tempPath)

    bucket.download_file_by_id(fileId, tempPath)
    util.uncompressAndDecrypt(conf, tempPath, destination)
    return 0


def uploadSecureFile(conf, bucket: Bucket, source):
    tempPath = os.path.join(conf.temp, os.path.basename(source))
    util.silentRemove(tempPath)

    util.compressAndEncrypt(tempPath)

    secureName = util.generateSecureName(source)
    uploadSource = UploadSourceLocalFile(source)
    bucket.upload(uploadSource, secureName)
    return 0
