import copy
from functools import total_ordering

from sqlalchemy import Column, Integer, String, Boolean, bindparam
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from utility import util
from utility.RWLock import RWLock
from utility.ResettingTimer import ResettingTimer

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
    remoteId = Column(String)
    remoteName = Column(String)
    status = Column(String)

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

    # can be decimals
    __IDLE_DELAY_SEC = 2
    __MAX_DELAY_SEC = 5

    def __init__(self, filename, source=None, forceUpload=False):
        self.filename = filename
        self.__files = None
        self.__sortedFiles = None
        self.__engine = create_engine('sqlite:///' + filename)
        Base.metadata.create_all(self.__engine)
        self.__sessionMaker = sessionmaker(bind=self.__engine)
        self.lock = RWLock()
        self.pendingActions = []
        self.idleTmr = None
        self.maxTmr = None
        self.source = source
        self.hasChanges = forceUpload

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
        def tmp():
            self.__addEntry(file)
        self.__readLock(tmp)

    def addorUpdate(self, file: IndexEntry):
        def tmp():
            self.__addOrUpdateEntry(file)
        self.__readLock(tmp)

    def addAll(self, files):
        def tmp():
            for f in files:
                self.__addEntry(f)
        self.__readLock(tmp)

    def remove(self, file: IndexEntry):
        def tmp():
            self.__removeEntry(file)
        self.__readLock(tmp)

    def removeAll(self, files):
        def tmp():
            for f in files:
                self.__removeEntry(f)
        self.__readLock(tmp)

    def clear(self):
        self.lock.reader_acquire()
        try:
            self.pendingActions.append(('t', None))
            self.__files.clear()
            self.__delayWrite()
        finally:
            self.lock.reader_release()
        self.__lazyLoad()

    def __readLock(self, func):
        self.__lazyLoad()
        self.lock.reader_acquire()
        try:
            func()
            self.__delayWrite()
        finally:
            self.lock.reader_release()

    def flush(self):
        self.__writePending()

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

    def __delayWrite(self):
        if self.idleTmr is None:
            self.idleTmr = ResettingTimer(self.__IDLE_DELAY_SEC, self.__writePending)
            self.idleTmr.start()
        else:
            self.idleTmr.reset()
        if self.maxTmr is None:
            self.maxTmr = ResettingTimer(self.__MAX_DELAY_SEC, self.__writePending)
            self.maxTmr.start()

    def __writePending(self):
        self.lock.writer_acquire()
        try:
            with self.__engine.begin() as conn:
                for type, data in self.pendingActions:
                    self.hasChanges = True
                    if type == 'a':
                        conn.execute(
                            IndexEntry.__table__
                                .insert(),
                            [data.__dict__])
                    elif type == 'd':
                        conn.execute(
                            IndexEntry.__table__
                                .delete()
                                .where(IndexEntry.path == bindparam('path')),
                            [{'path':data}])
                    elif type == 'u':
                        # update is very finicky, all of the properies passed to values have to exist in the db
                        # also the bindparam name can't exist in the object
                        conn.execute(
                            IndexEntry.__table__
                                .update()
                                .where(IndexEntry.path == bindparam('x1'))
                                .values(util.props(data)), # __dict__ doesnt work because SqlA creates extra properties
                            [{'x1':data.path}])
                    elif type == 't':
                        conn.execute('DELETE FROM ' + INDEX_TABLE_NAME)
                self.pendingActions.clear()
        finally:
            self.idleTmr = None
            self.maxTmr = None
            self.lock.writer_release()

    def __removeEntry(self, file):
        if isinstance(file, IndexEntry):
            path = file.path
        else:
            path = file

        if path in self.__files:
            del self.__files[path]
            self.pendingActions.append(('d', copy.copy(path)))

    def __addEntry(self, file):
        if file.path in self.__files:
            raise IndexException('File already exists in index: ' + file.path)
        self.__files[file.path] = file
        self.pendingActions.append(('a', copy.copy(file)))

    def __addOrUpdateEntry(self, file):
        action = 'u' if file.path in self.__files else 'a'
        self.__files[file.path] = file
        self.pendingActions.append((action, copy.copy(file)))
