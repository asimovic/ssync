from gzip import GzipFile
from utility.byte_buffer import Buffer

CHUNK = 16 * 1024


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

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__input = None
        self.__buf = None
        self.__gzip = None
        return

    def read (self, size=-1):
        # buffer is used to that the output of read can be exactly the number of bytes specified in 'size'
        while size < 0 or len(self.__buf) < size:
            data = self.__input.read(CHUNK)
            if not data:
                self.__gzip.close() # have to close stream otherwise it won't write the eof data
                break
            self.__gzip.write(data)
        return self.__buf.read(size)


class GzipDecompressStream(object):
    def __init__ (self, fileobj):
        """
        Create a new instance of a gzip stream from the stream-like input
        :param fileobj: stream-like object to compress or decompress
        """
        self.__buf = Buffer()
        self.__gzip = GzipFile(None, mode='rb', fileobj=fileobj)

    # included for 'with' support but not really a pythonic way of doing it.
    # using with is not required, passing this object is good enough
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__gzip.close()
        self.__gzip = None
        self.__buf = None
        return

    def read(self, size=-1):
        # buffer is used to that the output of read can be exactly the number of bytes specified in 'size'
        while size < 0 or len(self.__buf) < size:
            data = self.__gzip.read(CHUNK)
            if not data:
                break
            self.__buf.write(data)
        return self.__buf.read(size)