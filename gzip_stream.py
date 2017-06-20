from gzip import GzipFile
from collections import deque

CHUNK = 16 * 1024


class Buffer (object):
    def __init__ (self):
        self.__buf = deque()
        self.__size = 0

    def __len__ (self):
        return self.__size

    def write (self, data):
        self.__buf.append(data)
        self.__size += len(data)

    def read (self, size=-1):
        if size < 0:
            size = self.__size
        ret_list = []
        while size > 0 and len(self.__buf):
            s = self.__buf.popleft()
            size -= len(s)
            ret_list.append(s)
        if size < 0:
            ret_list[-1], remainder = ret_list[-1][:size], ret_list[-1][size:]
            self.__buf.appendleft(remainder)
        ret = b''.join(ret_list)
        self.__size -= len(ret)
        return ret

    def flush (self):
        pass

    def close (self):
        pass


class GzipCompressStream(object):
    def __init__ (self, fileobj, compresslevel=9):
        """
        Create a new instance of a gzip stream from the stream-like input
        :param fileobj: stream-like object to compress or decompress
        :param compresslevel: compression level (0-9) 9 is the highest compression, 0 is no compression,
                              only used for compression mode
        """
        self.__input = fileobj
        self.__buf = Buffer()
        self.__gzip = GzipFile(None, mode='wb', compresslevel=compresslevel, fileobj=self.__buf)

    #included for 'with' support but not really a pythonic way of doing it.
    #using with is not required, passing this object is good enough
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return

    def read (self, size=-1):
        while size < 0 or len(self.__buf) < size:
            s = self.__input.read(CHUNK)
            if not s:
                self.__gzip.close()
                break
            self.__gzip.write(s)
        return self.__buf.read(size)


class GzipDecompressStream(object):
    def __init__ (self, fileobj):
        """
        Create a new instance of a gzip stream from the stream-like input
        :param fileobj: stream-like object to compress or decompress
        """
        self.__gzip = GzipFile(None, mode='rb', fileobj=fileobj)

    #included for 'with' support but not really a pythonic way of doing it.
    #using with is not required, passing this object is good enough
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__gzip.close()
        return

    def read (self, size=-1):
        s = self.__gzip.read(size)
        if not s:
            self.__gzip.close()
        return s