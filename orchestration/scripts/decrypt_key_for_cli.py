import os
import stat
import tempfile
import re  # Add this import

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def main():
    raw_key = os.environ["SNOWFLAKE_PRIVATE_KEY"].strip()
    
    # FIX: If the key is missing internal newlines, reconstruct the PEM structure
    # This regex looks for the headers/footers and ensures there's a newline
    if "-----BEGIN" in raw_key and "\n" not in raw_key:
        print("Detected single-line key; repairing PEM framing...")
        raw_key = re.sub(r'(-----BEGIN RSA PRIVATE KEY-----)', r'\1\n', raw_key)
        raw_key = re.sub(r'(Proc-Type: 4,ENCRYPTED)', r'\1\n', raw_key)
        raw_key = re.sub(r'(DEK-Info: AES-256-CBC,.*)', r'\1\n\n', raw_key)
        raw_key = re.sub(r'([A-Za-z0-9+/]{64})', r'\1\n', raw_key)
        raw_key = re.sub(r'(-----END RSA PRIVATE KEY-----)', r'\n\1', raw_key)

    pem_data = raw_key.encode()
    passphrase = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"].encode()

    try:
        private_key = serialization.load_pem_private_key(
            pem_data, password=passphrase, backend=default_backend()
        )
    except Exception as e:
        # Diagnostic print to see what the library actually received
        print(f"FAILED to parse key. Raw string snippet: {raw_key[:50]}...")
        raise e

    unencrypted_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    fd, path = tempfile.mkstemp(suffix=".p8", prefix="sf_ci_key_")
    with os.fdopen(fd, "wb") as f:
        f.write(unencrypted_pem)
    os.chmod(path, stat.S_IRUSR)

    print(path) # This stdout is captured by KEY_PATH=$(...)

if __name__ == "__main__":
    main()
