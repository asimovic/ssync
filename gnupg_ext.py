import codecs
import threading
import os
import gnupg
import logging
from gnupg import GPG
from utility import util
from utility.byte_buffer import Buffer

log = logging.getLogger()


class GpgExtError(Exception):
    pass


class CryptExt(gnupg.Crypt):
    stderr = []
    pass


class GpgExt(GPG):
    """
    Allows stream-like objects to be encrypted and decrypted in chunks
    """
    pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__streamOpen = False
        self.__startedRead = False
        self.__writer = None
        self.__statusReader = None
        self.__instream = None
        self.__buf = None
        self.__process = None
        self.__stdin = None
        self.result = None

    CHUNK = 1024

    def encrypt_file(self, file, recipients, sign=None,
            always_trust=False, passphrase=None,
            armor=True, output=None, symmetric=False):
        raise GpgExtError('method not supported use encrypFileEx instead')


    def encrypFileExSymmetric(self, file, passphrase, symmetric=None, sign=None, output=None):
        symmetric = True if symmetric is None else symmetric
        return super().encrypt_file(file=file, recipients=None, sign=sign,
                                    always_trust=True, passphrase=passphrase,
                                    armor=False, output=output, symmetric=symmetric)

    def encrypFileEx(self, file, recipients, passphrase=None, sign=None, output=None):
        return super().encrypt_file(file=file, recipients=recipients, sign=sign,
                                    always_trust=True, passphrase=passphrase,
                                    armor=False, output=output, symmetric=False)

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
        return self

    def openEncryptStreamSymmetric(self, instream, passphrase, algorithm=None, sign=None, compress=True):
        args = ['--symmetric']
        if algorithm:
            # only works with symetric
            args.extend(['--cipher-algo', gnupg.no_quote(algorithm)])
            # else use the default, currently CAST5
        return self.__openEncryptStream(instream, args,
                                        sign=sign, passphrase=passphrase, compress=compress)

    def openEncryptStream(self, instream, recipients, sign=None, passphrase=None, compress=True):
        args = ['--encrypt']
        if not recipients:
            raise ValueError('No recipients specified')
        if not gnupg._is_sequence(recipients):
            recipients = (recipients,)
        for recipient in recipients:
            args.extend(['--recipient', gnupg.no_quote(recipient)])
        return self.__openEncryptStream(instream, args,
                                        sign=sign, passphrase=passphrase, compress=compress)

    def __openEncryptStream(self, instream, args, sign, passphrase, compress):
        if self.__streamOpen:
            return self
        if sign is True:  # pragma: no cover
            args.append('--sign')
        elif sign:  # pragma: no cover
            args.extend(['--sign', '--default-key', gnupg.no_quote(sign)])
        args.append('--always-trust')
        if not compress:
            args.extend(['--compress-algo', 'none'])
        self.__createStreamsAndProcess(instream, args, passphrase)
        return self

    def __createStreamsAndProcess(self, instream, args, passphrase):
        self.result = CryptExt(self) #self.result_map['crypt'](self)
        self.__instream = instream
        self.__buf = Buffer()
        self.__process = self._open_subprocess(args, passphrase is not None)
        self.__stdin = self.__process.stdin
        if passphrase:
            gnupg._write_passphrase(self.__stdin, passphrase, self.encoding)
        self.__streamOpen = True
        self.__startedRead = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__stdin.close()
        self.__process.stdout.close()
        self.__process.stderr.close()
        self.__process.kill()
        self.__process = None

        if self.__startedRead:
            self.__writer.join()
            self.__statusReader.join()
            self.__writer = None
            self.__statusReader = None
        self.__stdin = None
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
        while size < 0 or len(self.__buf) < size:
            data = stream.read(self.CHUNK)
            if len(data) == 0:
                break
            self.__buf.write(data)
        return self.__buf.read(size)

    def __startReadStderr(self):
        stderr = codecs.getreader(self.encoding)(self.__process.stderr)
        rr = threading.Thread(target=self._read_response, args=(stderr, self.result))
        rr.setDaemon(True)
        rr.start()
        return rr

    def _read_response(self, stream, result):
        # same as base class but with error handling
        lines = []
        while True:
            try:
                line = stream.readline()
                line = '' if line is None else line.rstrip()
                if len(line) == 0:
                    break
                lines.append(line)
                if self.verbose:  # pragma: no cover
                    print(line)
                if line[0:9] == '[GNUPG:] ':
                    # Chop off the prefix
                    line = line[9:]
                    L = line.split(None, 1)
                    keyword = L[0]
                    if len(L) > 1:
                        value = L[1]
                    else:
                        value = ""
                    result.handle_status(keyword, value)
            except ValueError: # buffer error occurs when reading the stream as its closed sometimes
                pass
            except Exception as e:
                line = 'Exception: ' + str(e)
                lines.append(line)
                log.exception('Failed to get stderr from gpg')
                break
        # python is so hacky
        if hasattr(result, 'stderr'):
            result.stderr.append(''.join(lines))
        log.trace(os.linesep.join(lines))