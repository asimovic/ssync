import base64
import os
import shutil
import threading
import gnupg_ext
from argon2_ext import ArgonHasher
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

__gpgLock = threading.Lock()

gpgCache = {}

def __getGpg(conf):
    global gpgCache
    tid = threading.get_ident()
    if tid in gpgCache:
        return gpgCache[tid]
    try:
        __gpgLock.acquire()
        if not tid in gpgCache:
            # seperate home paths to prevent crashes when encrypting
            homeDir = os.path.join(conf.GPGHome, str(tid))
            gpg = gnupg_ext.GpgExt(gnupghome=homeDir)
            with open(conf.GPGKeyFile, 'r') as keyFile:
                gpg.import_keys(keyFile.read())
            gpgCache[tid] = gpg
            return gpg
    finally:
        __gpgLock.release()

def cleanupGpg(conf):
    global gpgCache
    gpgCache = {}
    if os.path.exists(conf.GPGHome):
        shutil.rmtree(conf.GPGHome)

def generateSecureName(conf, filename):
    argonSalt = base64.b64decode(conf.ArgonSalt.encode('ascii'))
    h = ArgonHasher(time_cost=1, memory_cost=512, parallelism=2, salt_len=0)
    hs = h.hashWithFixedSalt(conf.SecureNameSalt + filename, argonSalt)
    #trim the argon details and salt, they should be constant anyway
    hs = hs[51:]
    return base64.b64encode(hs.encode('utf-8'), b'-_').decode('utf-8')

def compressAndEncrypt(conf, filename):
    p, h = compressAndEncryptWithHash(conf, filename, False)
    return p

tids = {}

def compressAndEncryptWithHash(conf, filename, computeHash=True):
    gpg = __getGpg(conf)
    tempPath = getTempPath(filename)
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
    gpg = __getGpg(conf)
    computeHash = False
    util.silentRemove(destination)

    with open(path, 'rb') as fin:
     with open(destination, 'wb') as fout:
      with gpg.openDecryptStream(fin, conf.args.passphrase) as din:
       with GzipDecompressStream(din) as gzip:
        with HashStream(gzip) as hin:
         hin = hin if computeHash else gzip
         shutil.copyfileobj(hin, fout)

def getTempPath(filePath):
    return filePath + util.APPLICATION_EXT
