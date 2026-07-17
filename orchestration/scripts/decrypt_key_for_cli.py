"""
Decrypts the passphrase-protected private key into a temporary
UNENCRYPTED PEM file that the Snowflake CLI's --private-key-file
option can use directly. Deleted by the workflow right after use.
"""
import os
import stat
import tempfile

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


def main():
    pem_data = os.environ["SNOWFLAKE_PRIVATE_KEY"].encode()
    passphrase = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"].encode()

    private_key = serialization.load_pem_private_key(
        pem_data, password=passphrase, backend=default_backend()
    )
    unencrypted_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    fd, path = tempfile.mkstemp(suffix=".p8", prefix="sf_ci_key_")
    with os.fdopen(fd, "wb") as f:
        f.write(unencrypted_pem)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

    print(path)


if __name__ == "__main__":
    main()
