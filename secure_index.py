import util
from b2.api import B2Api


class SecureIndex:
    def __init__(self, conf, api: B2Api, bucket_name):
        self.conf = conf
        self.api = api
        self.bucket_name = bucket_name

    def getIndex(self):
        #todo: use local config and only pull remote if no local
        #todo: add config to save file id and only pull if no id found
        fileId = None

        #get file id from name
        if not fileId:
            indexName = util.generateSecureName(self.bucket_name + '\index')
            bucket = self.api.get_bucket_by_name(self.bucket_name)
            bucketFiles = bucket.list_file_names(indexName, 1)
            if not bucketFiles['files']:
                raise Exception('index file not found in store')
            else:
                fileId = bucketFiles['files'][0]['fileId']

        return fileId
