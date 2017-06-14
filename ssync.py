import argparse
import json
import time

from sync import sync

import backblaze_b2
import config
from index.secure_index import IndexEntry
from index.secure_index_factory import SecureIndex, SecureIndexFactory
from sync import folder_parser
from sync.folder import LocalFolder, SecureFolder

CONFIG_PATH = 'ssync.conf'
REQUIRED_CONFIG = {'TempDir': str, 'GnuPGHome': str, 'IndexPath': str }
OPTIONAL_CONFIG = {'IndexFileId': str}


def createArgs():
    parser = argparse.ArgumentParser(description='Securely syncronize files between locations.')
    #parser.add_argument('src', help='source path to sync files from')
    #parser.add_argument('dest', help='destination path to sync files to')
    return parser


parser = createArgs()
args = parser.parse_args()

conf = config.readConfig(CONFIG_PATH,
                         'SSync',
                         REQUIRED_CONFIG,
                         OPTIONAL_CONFIG)
b2conf = config.readConfig(CONFIG_PATH,
                           'RemoteB2',
                           {'AccountId': str, 'ApplicationKey': str})

b2Api = backblaze_b2.setupApi(b2conf)


dest = folder_parser.parseSyncDir('b2://hello/folder/', None)
src = folder_parser.parseSyncDir('b2://as-Test01/', None)


s = SecureIndex(conf, b2Api, 'as-Test01')
s.getIndex()
