import hashlib
import logging.config
import os
from contextlib import contextmanager

APPLICATION_EXT = '.ssynctmp'

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
