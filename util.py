import hashlib
import os
import gzip
import base64

import shutil
from argon2 import PasswordHasher
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

SECURE_NAME_SALT = 'c3ViamVjdHM'

def generateSecureName(filename):
    h = PasswordHasher(time_cost=1, memory_cost=512, parallelism=2)
    hs = h.hash(SECURE_NAME_SALT + filename)
    return base64.b64encode(hs.encode('utf-8'), b'-_').decode('utf-8')

def compressAndEncrypt(conf, filename):
    tempPath = filename + '.ssync.tmp'
    silentRemove(tempPath)
    shutil.copy(filename, tempPath)
    return tempPath

def uncompressAndDecrypt(conf, path, destination):
    silentRemove(destination)
    shutil.move(path, destination)
    destination = None

def calculateHash(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

@contextmanager
def session_scope(sessionMaker):
    session = sessionMaker()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def silentRemove(filename):
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def getModTime(filepath):
    return int(round(os.path.getmtime(filepath) * 1000))

def checkDirectory(path):
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError:
            pass
    if not os.path.isdir(path):
        raise Exception('could not create directory %s' % (path,))

def normalizePath(path, isDir):
    if path == '':
        return path
    normalRelativePath = path.replace('\\', '/')
    if isDir and not normalRelativePath.endswith('/'):
        normalRelativePath += '/'
    return normalRelativePath
