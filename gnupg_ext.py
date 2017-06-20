import codecs
import threading

import gnupg
import sys
from gnupg import GPG

CHUNK = 1024


class GpgExtError(Exception):
    pass


class GpgExt(GPG):
    """
    Allows stream-like objects to be encrypted and decrypted in chunks
    """
    pass

    __streamOpen = False
    __startedRead = False
    __writer = None
    __statusReader = None

    def encrypt_file(self, file, recipients, sign=None,
            always_trust=False, passphrase=None,
            armor=True, output=None, symmetric=False):
        raise GpgExtError('method not supported use encrypFileEx instead')

    def encrypFileEx(self, file, recipients, sign=None, passphrase=None, output=None, symmetric=False):
        return super().encrypt_file(file=file, recipients=recipients, sign=sign,
                                    always_trust=True, passphrase=passphrase,
                                    armor=False, output=output, symmetric=symmetric)

    def openDecryptStream(self, instream, passphrase=None):
        """
        Configure this object as a decrypt stream. Spawns a Gpg sub process to handle the decrypting
        :param instream: input stream that contains encrypted data
        :param passphrase: passphrase to decrypt the data
        :return: status from subprocess, information will not be as complete as it is from the native functions
        """
        if self.__streamOpen:
            return self

        args = ["--decrypt"]
        # --always-trust needed to encrypt and decrypt without persistant home dir storage
        args.append("--always-trust")

        self.__createStreamsAndProcess(instream, args, passphrase)
        if passphrase:
            gnupg._write_passphrase(self.__stdin, passphrase, self.encoding)
        return self

    def openEncryptStream(self, instream, recipients, sign=None, passphrase=None, symmetric=False):
        if self.__streamOpen:
            return self

        args = ['--encrypt']
        if symmetric:
            # can't be False or None - could be True or a cipher algo value
            # such as AES256
            args = ['--symmetric']
            if symmetric is not True:
                args.extend(['--cipher-algo', gnupg.no_quote(symmetric)])
                # else use the default, currently CAST5
        else:
            if not recipients:
                raise ValueError('No recipients specified with asymmetric '
                                 'encryption')
            if not gnupg._is_sequence(recipients):
                recipients = (recipients,)
            for recipient in recipients:
                args.extend(['--recipient', gnupg.no_quote(recipient)])
        if sign is True:  # pragma: no cover
            args.append('--sign')
        elif sign:  # pragma: no cover
            args.extend(['--sign', '--default-key', gnupg.no_quote(sign)])
        args.append('--always-trust')
        self.__createStreamsAndProcess(instream, args, passphrase)
        return self

    def __createStreamsAndProcess(self, instream, args, passphrase):
        self.result = self.result_map['crypt'](self)
        self.__instream = instream
        self.__process = self._open_subprocess(args, passphrase is not None)
        self.__stdin = self.__process.stdin
        self.__streamOpen = True
        self.__startedRead = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__stdin is not None:
            try:
                self.__stdin.close()
            except IOError:
                pass
        self.__process.stdout.close()
        self.__process.stderr.close()
        self.__process.kill()
        self.__process = None

        self.__writer.join()
        self.__statusReader.join()
        self.__writer = None
        self.__statusReader = None
        self.__streamOpen = False
        self.__startedRead = False

    def read (self, size=-1):
        # spawn async writer on first run, writer will read from instream and fill the stdin buffer in gpg.exe
        # when we read from the gpg.exe it will allow more data to be written, buffer seems to be around 70mb in win32
        # maybe there is a way to limit runaway condidtions.
        # we need this because wrtiting and reading from std block if the buffers are full or there is no data
        if not self.__startedRead:
            self.__writer = gnupg._threaded_copy_data(self.__instream, self.__stdin)
            self.__statusReader = self.__startReadStderr()
            self.__startedRead = True

        stdout = self.__process.stdout
        return self.__readFromProcess(stdout, size)

    def __readFromProcess(self, stream, size):
        # Read the contents of the file from GPG's stdout
        # shouldn't block becuase the writer will keep writing until the instream is processed
        # when instream is closed this will closed as well
        read = 0
        chunks = []
        while size < 0 or read < size:
            data = stream.read(1024)
            l = len(data)
            if l == 0:
                break
            chunks.append(data)
            read += l
        return type(data)().join(chunks)

    def __startReadStderr(self):
        stderr = codecs.getreader(self.encoding)(self.__process.stderr)
        rr = threading.Thread(target=self._read_response, args=(stderr, self.result))
        rr.setDaemon(True)
        rr.start()
        return rr