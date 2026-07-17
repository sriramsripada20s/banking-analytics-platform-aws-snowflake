"""
Deploys orchestration/dbt_task_config.json to Snowflake by calling the
DEPLOY_DBT_TASK_DAG stored procedure.
"""
import base64
import json
import os
import sys

from cryptography.hazmat.primitives import serialization
import snowflake.connector


def load_private_key():
    b64_key = os.environ["SNOWFLAKE_PRIVATE_KEY"].strip()
    pem_bytes = base64.b64decode(b64_key)
    private_key = serialization.load_pem_private_key(pem_bytes, password=None)
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=load_private_key(),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )


def deploy_task_config(conn, config_path, feed_name):
    with open(config_path) as f:
        config = json.load(f)
    config_json_str = json.dumps(config)

    cur = conn.cursor()
    try:
        cur.execute(
            "CALL DEPLOY_DBT_TASK_DAG(%s, PARSE_JSON(%s))",
            (feed_name, config_json_str),
        )
        result = cur.fetchone()[0]
        print(f"Deployment result: {result}")
        return result
    finally:
        cur.close()


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "orchestration/dbt_task_config.json"
    feed_name = sys.argv[2] if len(sys.argv) > 2 else "fintech_pipeline"

    conn = connect()
    try:
        deploy_task_config(conn, config_path, feed_name)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
