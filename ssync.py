import argparse
import sys
import time
import os
import backblaze_b2
import security
import logging
import logging.config
from sync import folder_parser
from sync.sync import sync_folders
from utility import config
from utility import util
from utility import humanize

util.setupLogging('logging.conf')
log = logging.getLogger()

CONFIG_PATH = 'ssync.conf'
REQUIRED_CONFIG = {'TempDir': str, 'GPGHome': str, 'GPGKeyFile': str, 'GPGRecipient': str, 'IndexPath': str,
                   'LargeFileSize': str}
OPTIONAL_CONFIG = {'IndexFileId': str}

def createArgs():
    parser = argparse.ArgumentParser(description='Securely syncronize files between locations.',
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog, width=100))
    parser.add_argument('source', help='source path to sync files from')
    parser.add_argument('destination', help='destination path to sync files to')
    parser.add_argument('passphrase', help='passphrase for decryption')
    parser.add_argument('-k', '--keep', action='store_true',
                        help='keep files that the destination has if they do not exist on the source')
    parser.add_argument('--test', action='store_true',
                        help='run in test mode, operations are done against index only')
    parser.add_argument('--dryrun', action='store_true',
                        help='show output of what will happen without making any changes')
    parser.add_argument('-s', '--silent', action='store_true',
                        help='do not show progress while syncing')
    parser.add_argument('--workers',
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

    log.info('Reading configuration')
    conf = config.readConfig(CONFIG_PATH,
                             'SSync',
                             REQUIRED_CONFIG,
                             OPTIONAL_CONFIG)
    b2conf = config.readConfig(CONFIG_PATH,
                               'RemoteB2',
                               {'AccountId': str, 'ApplicationKey': str})

    conf.__setattr__('args', args)
    conf.args.workers = conf.args.workers or 10

    if not conf.args.exclude:
        conf.args.exclude = []
    if not conf.args.include:
        conf.args.include = []

    conf.__setattr__('largeFileBytes', humanize.human2bytes(conf.LargeFileSize))

    return conf, b2conf

def logException(exctype, value, tb):
    log.fatal(f'Unhandled exception ({exctype}): {value}{os.linesep}{tb}')
    pass

log.info('Starting ssync')
(conf, b2conf) = processConfig()

if conf.args.test:
    b2Api = None
else:
    b2Api = backblaze_b2.setupApi(b2conf)
    b2Api.set_thread_pool_size(conf.args.workers)

log.info(f'Starting sync from: {conf.args.source} to {conf.args.destination}')

try:
    source = folder_parser.parseSyncDir(conf.args.source, conf, b2Api)
    destination = folder_parser.parseSyncDir(conf.args.destination, conf, b2Api)
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

    config.writeConfigValue(CONFIG_PATH, 'SSync', 'IndexFileId', conf.IndexFileId)
    security.cleanupGpg()
except:
    log.exception('Sync failed')
    exit(1)