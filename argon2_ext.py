from argon2 import PasswordHasher, low_level, Type
from argon2._password_hasher import _ensure_bytes


class ArgonHasher(PasswordHasher):
    def hashWithFixedSalt(self, password, salt):
        """
        Hash *password* and return an encoded hash.

        :param password: Password to hash.
        :param salt: Password salt, should be array of bytes (generate using os.urandom)
        :type password: ``bytes`` or ``unicode``

        :raises argon2.exceptions.HashingError: If hashing fails.

        :rtype: unicode
        """
        return low_level.hash_secret(
            secret=_ensure_bytes(password, self.encoding),
            salt=salt,
            time_cost=self.time_cost,
            memory_cost=self.memory_cost,
            parallelism=self.parallelism,
            hash_len=self.hash_len,
            type=Type.I,
        ).decode("ascii")