from collections import deque

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