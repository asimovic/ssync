import base64
import shutil
import threading

import time
from argon2 import PasswordHasher

import gnupg_ext
from utility import util
from utility.gzip_stream import GzipCompressStream
from utility.gzip_stream import GzipDecompressStream
from utility.hash_stream import HashStream


class Passthrough(object):
    def __init__(self, obj):
        self.passObj = obj
    def __enter__(self):
        return self.passObj
    def __exit__(self, exc_type, exc_value, traceback):
        return self.passObj.__exit__(exc_type, exc_value, traceback)


__SECURE_NAME_SALT = 'c3ViamVjdHM'
__gpgLock = threading.Lock()

gpg = None

def __setupGpg(conf):
    global gpg
    if gpg is None:
        try:
            __gpgLock.acquire()
            if gpg is None:
                gpg = gnupg_ext.GpgExt(gnupghome=conf.GPGHome)
                with open(conf.GPGKeyFile, 'r') as keyFile:
                    gpg.import_keys(keyFile.read())
        finally:
            __gpgLock.release()

def cleanupGpg():
    global gpg
    if gpg is not None:
        shutil.rmtree(gpg.gnupghome)
    gpg = None

def generateSecureName(filename):
    h = PasswordHasher(time_cost=1, memory_cost=512, parallelism=2)
    hs = h.hash(__SECURE_NAME_SALT + filename)
    #trim the argon details, they should be constant anyway
    hs = hs[28:]
    return base64.b64encode(hs.encode('utf-8'), b'-_').decode('utf-8')

def compressAndEncrypt(conf, filename):
    p, h = compressAndEncryptWithHash(conf, filename, False)
    return p

def compressAndEncryptWithHash(conf, filename, computeHash=True):
    global gpg
    __setupGpg(conf)
    tempPath = filename + util.APPLICATION_EXT
    util.silentRemove(tempPath)

    with open(filename, 'rb') as fin:
     with open(tempPath, 'wb') as fout:
      with HashStream(fin) as hin:
       hin = hin if computeHash else fin
       with GzipCompressStream(hin) as gzip:
        with gpg.openEncryptStream(gzip, conf.GPGRecipient, compress=False) as ein:
         shutil.copyfileobj(ein, fout)
       hashDigest = hin.hexdigest() if computeHash else None

    return tempPath, hashDigest

def decompressAndDecrypt(conf, path, destination):
    __setupGpg(conf)
    computeHash = False
    util.silentRemove(destination)

    with open(path, 'rb') as fin:
     with open(destination, 'wb') as fout:
      with gpg.openDecryptStream(fin, conf.args.passphrase) as din:
       with GzipDecompressStream(din) as gzip:
        with HashStream(gzip) as hin:
         hin = hin if computeHash else gzip
         shutil.copyfileobj(hin, fout)