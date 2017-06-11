import os
from collections import OrderedDict

import util

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class IndexException(Exception):
    pass

Base = declarative_base()

class IndexEntry(Base):
    """
    Holds information about one file in a the index.
    The path is relative to the root in all cases.
    """

    __tablename__ = 'files'
    path = Column(String, primary_key=True)
    size = Column(Integer)
    modTime = Column(Integer)
    md5 = Column(String)
    b2Id = Column(String)
    b2Name = Column(String)

    def __init__(self, path, size, modTime, md5, b2Id, b2Name):
        self.path = path
        self.size = size
        self.modTime = modTime
        self.md5 = md5
        self.b2Id = b2Id
        self.b2Name = b2Name

    def __repr__(self):
        return 'File(%s)' % self.path

class SecureIndex:

    def __init__(self, filename):
        self.filename = filename
        self.__files = None
        self.__sortedFiles = None
        engine = create_engine('sqlite:///' + filename)
        Base.metadata.create_all(engine)
        self.__sessionMaker = sessionmaker(bind=engine)


    def getAll(self):
        self.__lazyLoad()
        if self.__sortedFiles is None:
            # Sort files by key (filepath), and return list of values (IndexEntry)
            self.__sortedFiles = [v for (k, v) in sorted(self.__files.items())]

        return self.__sortedFiles

    def add(self, file: IndexEntry):
        self.__lazyLoad()
        with util.session_scope(self.__sessionMaker) as session:
            self.__addEntry(file, session)

    def addAll(self, files):
        self.__lazyLoad()
        with util.session_scope(self.__sessionMaker) as session:
            for f in files:
                self.__addEntry(f, session)

    def remove(self, file: IndexEntry):
        self.__lazyLoad()
        with util.session_scope(self.__sessionMaker) as session:
            self.__removeEntry(file, session)

    def removeAll(self, files):
        self.__lazyLoad()
        with util.session_scope(self.__sessionMaker) as session:
            for f in files:
                self.__removeEntry(f, session)


    def __lazyLoad(self):
        # Files are being updates so clear the sorted cache
        self.__sortedFiles = None
        if self.__files is None:
            self.__files = {}
            with util.session_scope(self.__sessionMaker) as session:
                for f in session.query(IndexEntry):
                    self.__files[f.path] = f

    def __removeEntry(self, file, session):
        if file.path in self.__files:
            del self.__files[file.path]
            session.remove(file)

    def __addEntry(self, file, session):
        if file.path in self.__files:
            raise IndexException('File already exists in index: ' + file.path)
        self.__files[file.path] = file
        session.add(file)

