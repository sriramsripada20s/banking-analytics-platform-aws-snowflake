"""
Decrypts the passphrase-protected private key (from GitHub Secrets, via
env vars) into a temporary UNENCRYPTED PEM file that the Snowflake CLI's
--private-key-file option can consume directly.
"""
import os
import stat
import tempfile

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


def main():
    pem_data_str = os.environ["SNOWFLAKE_PRIVATE_KEY"]

    print(f"[DIAGNOSTIC] Key string length: {len(pem_data_str)} characters")
    print(f"[DIAGNOSTIC] Number of lines: {pem_data_str.count(chr(10)) + 1}")
    print(f"[DIAGNOSTIC] Starts with '-----BEGIN': {pem_data_str.lstrip().startswith('-----BEGIN')}")
    print(f"[DIAGNOSTIC] Contains 'ENCRYPTED': {'ENCRYPTED' in pem_data_str}")
    print(f"[DIAGNOSTIC] Last 40 chars (structural only): {repr(pem_data_str.rstrip()[-40:])}")
    print(f"[DIAGNOSTIC] First 40 chars (structural only): {repr(pem_data_str[:40])}")
    print(f"[DIAGNOSTIC] Contains carriage returns (\\r): {chr(13) in pem_data_str}")

    pem_data = pem_data_str.encode()
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
