import os
import util

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from functools import total_ordering

class IndexException(Exception):
    pass

INDEX_TABLE_NAME = 'files'

Base = declarative_base()

@total_ordering
class IndexEntry(Base):
    """
    Holds information about one file in a the index.
    The path is relative to the root in all cases.
    """

    __tablename__ = INDEX_TABLE_NAME
    path = Column(String, primary_key=True)
    isDir = Column(Boolean)
    size = Column(Integer)
    modTime = Column(Integer)
    hash = Column(String)
    b2Id = Column(String)
    b2Name = Column(String)

    def __init__(self, path, isDir, size, modTime, hash, remoteId, remoteName):
        self.path = path
        self.isDir = isDir
        self.size = size
        self.modTime = modTime
        self.hash = hash
        self.remoteId = remoteId
        self.remoteName = remoteName

    def __eq__(self, other):
        return self.isDir == other.isDir and \
               self.path.lower() == other.path.lower()

    def __lt__(self, other):
        if isinstance(other, str):
            path2 = other
        else:
            path2 = other.path

        # Depth first sorting, works but makes too many other things more complicated.
        # Use simple string comparison on path
        # d1 = self.path.count('/')
        # d2 = path2.count('/')
        #
        # if d1 > d2:
        #     return not self.path.startswith(path2)
        # return self.path.lower() < path2.lower()
        return self.path.lower() < path2.lower()

    def __repr__(self):
        return f'Index: {self.path}'

class SecureIndex:

    def __init__(self, filename):
        self.filename = filename
        self.__files = None
        self.__sortedFiles = None
        engine = create_engine('sqlite:///' + filename)
        Base.metadata.create_all(engine)
        self.__sessionMaker = sessionmaker(bind=engine)


    def get(self, path):
        self.__lazyLoad(False)
        if path in self.__files:
            return self.__files[path]
        return None

    def getAll(self):
        self.__lazyLoad(False)
        if self.__sortedFiles is None:
            # Sort files by key (filepath), and return list of values (IndexEntry)
            self.__sortedFiles = [v for (k, v) in sorted(self.__files.items(), key=lambda x: x[1])]
        return self.__sortedFiles

    def add(self, file: IndexEntry):
        self.__lazyLoad()
        with util.session_scope(self.__sessionMaker) as session:
            self.__addEntry(file, session)
            session.expunge_all()

    def addAll(self, files):
        self.__lazyLoad()
        with util.session_scope(self.__sessionMaker) as session:
            for f in files:
                self.__addEntry(f, session)
            session.expunge_all()

    def remove(self, file: IndexEntry):
        self.__lazyLoad()
        with util.session_scope(self.__sessionMaker) as session:
            self.__removeEntry(file, session)

    def removeAll(self, files):
        self.__lazyLoad()
        with util.session_scope(self.__sessionMaker) as session:
            for f in files:
                self.__removeEntry(f, session)

    def clear(self):
        with util.session_scope(self.__sessionMaker) as session:
            session.execute('DELETE FROM ' + INDEX_TABLE_NAME)
        self.__files = None
        self.__lazyLoad()


    def __lazyLoad(self, updating=True):
        # Files are being updates so clear the sorted cache
        if updating:
            self.__sortedFiles = None
        if self.__files is None:
            self.__files = {}
            with util.session_scope(self.__sessionMaker) as session:
                for f in session.query(IndexEntry):
                    self.__files[f.path] = f
                session.expunge_all()

    def __removeEntry(self, file, session):
        if file.path in self.__files:
            del self.__files[file.path]
            session.remove(file)

    def __addEntry(self, file, session):
        if file.path in self.__files:
            raise IndexException('File already exists in index: ' + file.path)
        self.__files[file.path] = file
        session.add(file)

