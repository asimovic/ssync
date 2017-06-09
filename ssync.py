import os, sys, argparse
from b2.account_info.sqlite_account_info import (SqliteAccountInfo)
from b2.api import (B2Api, B2RawApi)
from b2.b2http import (B2Http)
from b2.cache import (AuthInfoCache)
from secure_index import SecureIndex
import backblaze_b2

def createArgs():
    parser = argparse.ArgumentParser(description='Securely syncronize files between locations.')
    #parser.add_argument('src', help='source path to sync files from')
    #parser.add_argument('dest', help='destination path to sync files to')
    return parser


parser = createArgs()
args = parser.parse_args()

info = SqliteAccountInfo('b2_account_info')
b2Http = B2Http()
rawApi = B2RawApi(b2Http)
b2Api = B2Api(info, AuthInfoCache(info), raw_api=rawApi)
backblaze_b2.authorizeAccount(b2Api, 'df2e5c3d73ad', '001a6e9efd45ea3dd77dc1999ec47fd904bde36c65')

s = SecureIndex(b2Api, 'as-Test01')
s.getIndex()
