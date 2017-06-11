import os
import gzip
import base64

from argon2 import PasswordHasher
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

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

    silentRemove(destination)
    destination = None


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


def getModTime(filepath):
    return int(round(os.path.getmtime(filepath) * 1000))
