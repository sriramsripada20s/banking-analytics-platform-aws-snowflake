"""
Writes the base64-encoded private key (from GitHub Secrets) to a temp
PEM file that the Snowflake CLI's --private-key-file option can use.
No passphrase — GitHub Secrets encryption at rest is the protection.
"""
import base64
import os
import stat
import tempfile


def main():
    b64_key = os.environ["SNOWFLAKE_PRIVATE_KEY"].strip()
    pem_bytes = base64.b64decode(b64_key)

    fd, path = tempfile.mkstemp(suffix=".p8", prefix="sf_ci_key_")
    with os.fdopen(fd, "wb") as f:
        f.write(pem_bytes)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

    print(path)


if __name__ == "__main__":
    main()
