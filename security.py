import gnupg_ext
import util
import base64
import shutil
import threading

from argon2 import PasswordHasher
from gzip_stream import GzipCompressStream
from gzip_stream import GzipDecompressStream

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

def generateSecureName(filename):
    h = PasswordHasher(time_cost=1, memory_cost=512, parallelism=2)
    hs = h.hash(__SECURE_NAME_SALT + filename)
    return base64.b64encode(hs.encode('utf-8'), b'-_').decode('utf-8')

def compressAndEncrypt(conf, filename):
    global gpg
    __setupGpg(conf)
    tempPath = filename + util.APPLICATION_EXT
    # util.silentRemove(tempPath)
    #
    # with open(filename, 'rb') as source:
    #     with open(tempPath, 'wb') as destination:
    #         with GzipCompressStream(source) as gzip:
    #             shutil.copyfileobj(gzip, destination)
    #
    with open(tempPath, 'rb') as fin:
        with open(tempPath+'2', 'wb') as fout:
            with gpg.openEncryptStream(fin, recipients=['none@none.com']) as ein:
                shutil.copyfileobj(ein, fout)

    with open(tempPath+'2', 'rb') as fin:
        with open(tempPath+'3', 'wb') as fout:
            with gpg.openDecryptStream(fin, conf.args.passphrase) as din:
                shutil.copyfileobj(din, fout)

    # with open(tempPath+'2', 'rb') as f:
    #     status = gpg.decrypt_file(
    #         f, passphrase=conf.args.passphrase)

    return tempPath

def decompressAndDecrypt(conf, path, destination):
    __setupGpg(conf)
    util.silentRemove(destination)
    with open(path, 'rb') as source:
        with open(destination, 'wb') as destination:
            with GzipDecompressStream(source) as gzip:
                shutil.copyfileobj(gzip, destination)
    destination = None