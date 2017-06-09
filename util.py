from argon2 import PasswordHasher
import os
import gzip
import base64
import shutil

SECURE_NAME_SALT = 'c3ViamVjdHM'


def generateSecureName(filename):
    h = PasswordHasher(time_cost=1, memory_cost=512, parallelism=2)
    hs = h.hash(SECURE_NAME_SALT + filename)
    return base64.b64encode(hs.encode('utf-8'), b'-_').decode('utf-8')


def compressAndEncrypt(conf, filename):
    tempPath = os.path.join(conf.temp, os.path.basename(filename))
    silentRemove(tempPath)
    return tempPath


def uncompressAndDecrypt(conf, filename, destination):
    destination = None


def silentRemove(filename):
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass
