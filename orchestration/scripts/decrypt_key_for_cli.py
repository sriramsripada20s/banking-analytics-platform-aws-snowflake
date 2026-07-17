import os
import stat
import tempfile
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def main():
    # 1. Get the base64 encoded string from environment
    b64_key = os.environ["SNOWFLAKE_PRIVATE_KEY"].strip()
    
    # 2. Decode it back to the original PEM bytes
    pem_data = base64.b64decode(b64_key)
    
    # 3. Decrypt
    passphrase = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"].encode()
    
    private_key = serialization.load_pem_private_key(
        pem_data, 
        password=passphrase, 
        backend=default_backend()
    )
    
    # 4. Export as unencrypted PKCS8 for Snowflake CLI
    unencrypted_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    # 5. Save to temp file
    fd, path = tempfile.mkstemp(suffix=".p8", prefix="sf_ci_key_")
    with os.fdopen(fd, "wb") as f:
        f.write(unencrypted_pem)
    os.chmod(path, stat.S_IRUSR)

    print(path) # Only this line goes to stdout

if __name__ == "__main__":
    main()
