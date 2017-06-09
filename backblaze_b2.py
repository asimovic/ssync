import os
import util
from b2.exception import B2Error
from b2.api import Bucket
from b2.upload_source import UploadSourceLocalFile


def authorizeAccount(api, accountId, applicationKey):
    try:
        api.authorize_account('production', accountId, applicationKey)
        return 0
    except B2Error as e:
        print('ERROR: unable to authorize account: ' + str(e))
        return 1


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
