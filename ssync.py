import argparse
import sys
import time
import os
import backblaze_b2
import security
import logging
import logging.config
from index import index_verficiation
from index.secure_index_factory import IndexFactoryException
from sync import folder_parser
from sync.folder import SecureFolder
from sync.sync import sync_folders
from utility import config
from utility import util
from utility import humanize

util.setupLogging('logging.conf')
log = logging.getLogger()

CONFIG_PATH = 'ssync.conf'
REQUIRED_CONFIG = {'TempDir': str, 'GPGHome': str, 'GPGKeyFile': str, 'GPGRecipient': str, 'IndexPath': str,
                   'LargeFileSize': str}
OPTIONAL_CONFIG = {}

def createArgs():
    parser = argparse.ArgumentParser(description='Securely syncronize files between locations.',
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog, width=100))
    parser.add_argument('-s', '--source', help='source path to sync files from')
    parser.add_argument('-d', '--destination', help='destination path to sync files to')
    parser.add_argument('passphrase', help='passphrase for decryption')
    parser.add_argument('-k', '--keep', action='store_true',
                        help='keep files that the destination has if they do not exist on the source')
    parser.add_argument('--test', action='store_true',
                        help='run in test mode, all operations are only done locally')
    parser.add_argument('--testIndex', action='store_true',
                        help='run in test mode, operations are done against index only')
    parser.add_argument('--dryrun', action='store_true',
                        help='show output of what will happen without making any changes')
    parser.add_argument('-vi', '--validateIndex',
                        help='validate and update the index on the remote folder, does not run sync')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='do not show progress while syncing')
    parser.add_argument('--uploadIndex',
                        help='uploads the local index to the remote, debug use only')
    parser.add_argument('-w', '--workers', type=int,
                        help='max number of worker threads for searching and uploading')
    parser.add_argument('--exclude', nargs='+',
                        help="""ignore files that match the given pattern. The pattern is 
                                a regular expression that is tested against the full path of each file""")
    parser.add_argument('--include', nargs='+',
                        help="""override ignoring files that match the given pattern. The pattern is 
                                a regular expression that is tested against the full path of each file""")
    parser.add_argument('--comparison', type=int, default=4,
                        help="""comaprison level to use when determining if files are the same.
                                1 - name only
                                2 - name and size
                                3 - name, size and timestamp
                                4 - name, size, timestamp and hash""")
    return parser

def processConfig():
    log.info('Parsing Arguments')
    parser = createArgs()
    args = parser.parse_args()
    if args.testIndex:
        args.test = True

    if args.test:
        log.info('Running in test mode.')

    log.info('Reading configuration')
    conf = config.readConfig(CONFIG_PATH,
                             'SSync',
                             REQUIRED_CONFIG,
                             OPTIONAL_CONFIG)
    b2conf = config.readConfig(CONFIG_PATH,
                               'RemoteB2',
                               {'AccountId': str, 'ApplicationKey': str})

    conf.__setattr__('args', args)
    conf.args.workers = conf.args.workers or 20

    if not conf.args.exclude:
        conf.args.exclude = []
    if not conf.args.include:
        conf.args.include = []

    conf.__setattr__('largeFileBytes', humanize.human2bytes(conf.LargeFileSize))

    return conf, b2conf

def logException(exctype, value, tb):
    log.fatal(f'Unhandled exception ({exctype}): {value}{os.linesep}{tb}')
    pass

def runSync(conf, api):
    log.info(f'Starting sync from: {conf.args.source} to {conf.args.destination}')

    try:
        source = folder_parser.parseSyncDir(conf.args.source, conf, api)
        destination = folder_parser.parseSyncDir(conf.args.destination, conf, api)
    except:
        log.exception('Invalid source or destination')
        exit(1)

    try:
        sync_folders(
            source_folder=source,
            dest_folder=destination,
            now_millis=int(round(time.time() * 1000)),
            stdout=sys.stdout,
            conf=conf
        )
    except:
        log.exception('Sync failed')
        exit(1)

def runValidation(conf, api):
    log.info(f'Starting index validation on: {conf.args.validateIndex} (files only)')

    try:
        remote = folder_parser.parseSyncDir(conf.args.validateIndex, conf, api)
        if not isinstance(remote, SecureFolder):
            log.error('Invalid remote path for validateIndex, path doesn\'t support SecureIndex')
            exit(1)
    except:
        log.exception('Invalid remote path for validateIndex')
        exit(1)

    index_verficiation.ValidateAndUpdateIndex(remote.bucket, remote.path, remote.secureIndex)
    remote.secureIndex.source.uploadIndex(remote.secureIndex)
    return

def runUploadIndex(conf, api):
    log.info(f'Starting index upload to: {conf.args.uploadIndex}')

    try:
        remote = folder_parser.parseSecureB2Folder(conf.args.uploadIndex, conf, api, True)
        if not isinstance(remote, SecureFolder):
            log.error('Invalid remote path for uploadIndex, path doesn\'t support SecureIndex')
            exit(1)
    except IndexFactoryException:
        log.exception('Local index not found')
        exit(1)
    except:
        log.exception('Invalid remote path for uploadIndex')
        exit(1)

    remote.secureIndex.forceUpload = True
    remote.secureIndex.source.uploadIndex(remote.secureIndex)
    return

log.info('Starting ssync')
(conf, b2conf) = processConfig()

if conf.args.test:
    b2Api = None
else:
    log.info('Starting b2 api')
    b2Api = backblaze_b2.setupApi(b2conf)
    b2Api.set_thread_pool_size(conf.args.workers)

if not os.path.exists(conf.GPGKeyFile):
    log.error('GPG key file not found at: ' + conf.GPGKeyFile)
    exit(1)

if conf.args.validateIndex:
    runValidation(conf, b2Api)
elif conf.args.uploadIndex:
    runUploadIndex(conf, b2Api)
else:
    runSync(conf, b2Api)

security.cleanupGpg(conf)