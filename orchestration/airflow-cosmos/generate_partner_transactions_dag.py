"""
===============================================================================
AIRFLOW DAG: generate_partner_transactions_dag
===============================================================================
ARCHITECTURE OVERVIEW:
This DAG manages the end-to-end ingestion lifecycle for Partner Transactions:

1. PHASE 1 (Generation & S3 Landing):
   - Airflow triggers an AWS Lambda function to generate mock/real partner
     transaction data and write it to Amazon S3.
   - Airflow inspects S3 to confirm the file actually landed successfully.

2. PHASE 2 (Snowpipe Continuous Loading + Dynamic Table Validation):
   - Once the file is in S3, Snowpipe automatically loads it into Snowflake's 
     raw landing table (FINTECH_PROD.RAW.PARTNER_TRANSACTIONS) in real time.
   - Snowflake Dynamic Tables (CLEAN vs QUARANTINE) automatically separate
     valid transactions from malformed/null records in the background.
   - Airflow's secondary job is simply to check the QUARANTINE table's row count 
     and send a Slack notification detailing pipeline health.
===============================================================================
"""

import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

# Initialize standard logging for Airflow task executions
logger = logging.getLogger("airflow.task")

# Global Configuration Constants
LAMBDA_FUNCTION_NAME = "fintech-data-generator"
BUCKET = "fintech-project-sriram2026"
SNOWFLAKE_CONN_ID = "snowflake_fintech"


# =============================================================================
# HELPER FUNCTIONS: CLIENT & CONNECTION BUILDERS
# =============================================================================

def _get_lambda_client():
    """Retrieves AWS credentials from Airflow Connections and builds a boto3 Lambda client."""
    import boto3
    aws_conn = BaseHook.get_connection("aws_fintech")
    return boto3.client(
        "lambda",
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name="us-east-1",
    )


def _get_s3_client():
    """Retrieves AWS credentials from Airflow Connections and builds a boto3 S3 client."""
    import boto3
    aws_conn = BaseHook.get_connection("aws_fintech")
    return boto3.client(
        "s3",
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name="us-east-1",
    )


def _get_snowflake_connection():
    """Retrieves Snowflake credentials from Airflow Connections and creates a Python connector."""
    import snowflake.connector
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson
    return snowflake.connector.connect(
        account=extra["account"],
        user=conn.login,
        password=conn.password,
        warehouse=extra["warehouse"],
        database=extra["database"],
        role=extra["role"],
        schema="GOVERNANCE",
    )


# =============================================================================
# HELPER FUNCTIONS: NOTIFICATIONS & ALERTS
# =============================================================================

def _slack_alert(text: str):
    """Sends a formatted text message to your team's Slack channel via webhook."""
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        SlackWebhookHook(slack_webhook_conn_id="slack_default").send_text(text)
    except Exception as e:
        logger.error("Slack alert failed: %s", e)


def task_failure_slack_alert(context):
    """
    DAG Failure Callback:
    Acts as a safety net that automatically triggers if ANY task crashes.
    Sends full context details (DAG ID, Task ID, exception traceback) to Slack.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"
    dag_id = ti.dag_id if ti else "unknown"
    run_id = context.get("run_id", "unknown")
    exception = context.get("exception", "No exception details available")

    _slack_alert(
        f":rotating_light: *Task crashed*: `{dag_id}.{task_id}`\n"
        f"*Run:* {run_id}\n"
        f"```{exception}```"
    )


# =============================================================================
# HELPER FUNCTIONS: VERIFICATION & LAMBDA INVOCATION
# =============================================================================

def _verify_s3_file_written(source_name, s3_prefix, uses_date_subfolder=True):
    """
    Scans S3 to ensure that new data was actually written by the Lambda function.
    Raises a ValueError (which fails the Airflow task) if no file was created today.
    """
    from datetime import datetime, timezone
    s3_client = _get_s3_client()
    today_date = datetime.now(timezone.utc).date()

    if uses_date_subfolder:
        # For partition structures like: s3://bucket/prefix/YYYY/MM/DD/
        today_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        full_prefix = f"{s3_prefix.rstrip('/')}/{today_str}/"
        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=full_prefix)
        found = response.get("KeyCount", 0) > 0
    else:
        # For flat structures like: s3://bucket/prefix/filename.json
        full_prefix = f"{s3_prefix.rstrip('/')}/"
        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=full_prefix)
        contents = response.get("Contents", [])
        found = any(obj["LastModified"].date() == today_date for obj in contents)

    if not found:
        _slack_alert(
            f":red_circle: *S3 write verification failed* for {source_name}\n"
            f"No file modified today under `{full_prefix}`"
        )
        raise ValueError(f"{source_name}: no file modified today found under {full_prefix}")

    return True


def _invoke_lambda(payload, source_name, s3_prefix, uses_date_subfolder=True):
    """
    Invokes the AWS Lambda data generator and verifies S3 landing.
    Triggers Slack notifications for success or failure.
    """
    client = _get_lambda_client()
    
    # 1. Trigger AWS Lambda execution
    response = client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )
    result = json.loads(response["Payload"].read())

    # 2. Check if Lambda threw an internal execution error
    if response.get("StatusCode") != 200 or "errorMessage" in result:
        _slack_alert(f":red_circle: *Lambda invoke failed* for {source_name}\n```{result}```")
        raise ValueError(f"Lambda invocation failed for {source_name}: {result}")

    # 3. Double-check that the generated file actually landed in S3
    _verify_s3_file_written(source_name, s3_prefix, uses_date_subfolder=uses_date_subfolder)
    
    # 4. Notify Slack that data is in S3 and waiting for Snowpipe auto-ingestion
    _slack_alert(f":package: *{source_name}* generated and confirmed in S3 (Snowpipe will auto-load)")
    return result


def check_partner_transactions_quarantine():
    """
    Queries Snowflake to see if any malformed records were routed to 
    PARTNER_TRANSACTIONS_QUARANTINE in the last 24 hours, alerting Slack with results.
    """
    conn = _get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM FINTECH_PROD.GOVERNANCE.PARTNER_TRANSACTIONS_QUARANTINE
            WHERE _loaded_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
        """)
        count = cur.fetchone()[0]

        # Send status alert to Slack based on quarantine check count
        if count > 0:
            _slack_alert(f":warning: *partner_transactions*: {count} row(s) quarantined in the last day")
        else:
            _slack_alert(f":white_check_mark: *partner_transactions*: quarantine clean")
            
        return count
    finally:
        conn.close()


# =============================================================================
# AIRFLOW DAG DEFINITION
# =============================================================================

@dag(
    dag_id="generate_partner_transactions_dag",
    schedule="0 */4 * * *",  # Runs every 4 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,  # Prevents backfilling historic runs on creation
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": task_failure_slack_alert, # Auto-alert Slack on task crash
    },
    tags=["ingestion", "fintech", "generation", "phase1", "phase2"],
)
def generate_partner_transactions_dag():

    @task
    def generate_partner_transactions():
        """Task 1: Generate batch data via Lambda and verify write to S3."""
        return _invoke_lambda(
            {"dataset": "partner_transactions", "rows": 50000, "bucket": BUCKET},
            "partner_transactions",
            s3_prefix="partner_transactions",
            uses_date_subfolder=False,
        )

    @task
    def check_quarantine(_prev):
        """
        Task 2: Inspect Snowflake quarantine table for bad rows.
        Takes '_prev' as an argument to enforce explicit Airflow task dependency.
        """
        return check_partner_transactions_quarantine()

    # Enforce task execution order: generate_partner_transactions MUST succeed before check_quarantine runs
    check_quarantine(generate_partner_transactions())


# Instantiate the DAG
generate_partner_transactions_dag()