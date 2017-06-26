import base64
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


__SECURE_NAME_SALT = 'c3ViamVjdHM'
__ARGON_SALT = b'\xa3\xfe\xc1eZ\xb6\xe3T\x08w\xb1?E\xc3\xd7\xd1'
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
    h = ArgonHasher(time_cost=1, memory_cost=512, parallelism=2, salt_len=0)
    hs = h.hashWithFixedSalt(__SECURE_NAME_SALT + filename, __ARGON_SALT)
    #trim the argon details and salt, they should be constant anyway
    hs = hs[51:]
    return base64.b64encode(hs.encode('utf-8'), b'-_').decode('utf-8')

def compressAndEncrypt(conf, filename):
    p, h = compressAndEncryptWithHash(conf, filename, False)
    return p

def compressAndEncryptWithHash(conf, filename, computeHash=True):
    global gpg
    __setupGpg(conf)
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

def getTempPath(filePath):
    return filePath + util.APPLICATION_EXT
