import hashlib

class HashStream:
    def __init__(self, instream, hashObj=None):
        """
        Create a new hashing object that can process streams.
        :param instream: file-like stream object that will be hashed
        :param hashObj: hashlib object that can generate a hash (ex. hashlib.md5, hashlib.sha256)
        """
        self.__instream = instream
        if not hashObj:
            self.__hashObj = hashlib.md5()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__instream = None
        self.__hashObj = None

    def digest(self):
        return self.__hashObj.digest()

    def hexdigest(self):
        return self.__hashObj.hexdigest()

    def read(self, size=-1):
        # pass through read size, we're trusting the other streams can throttle their data reading
        data = self.__instream.read(size)
        if data:
            self.__hashObj.update(data)
        return data