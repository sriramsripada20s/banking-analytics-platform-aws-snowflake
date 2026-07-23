"""
PHASE 1 + 2: Replaces EventBridge and the retired Stream+Task CDC
pipeline for account_activity. Generates via Lambda, verifies S3,
then validates and loads via the batch pattern.
"""
import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

logger = logging.getLogger("airflow.task")
LAMBDA_FUNCTION_NAME = "fintech-data-generator"
BUCKET = "fintech-project-sriram2026"
SNOWFLAKE_CONN_ID = "snowflake_fintech"


def _get_lambda_client():
    import boto3
    aws_conn = BaseHook.get_connection("aws_fintech")
    return boto3.client("lambda", aws_access_key_id=aws_conn.login,
                         aws_secret_access_key=aws_conn.password, region_name="us-east-1")


def _get_s3_client():
    import boto3
    aws_conn = BaseHook.get_connection("aws_fintech")
    return boto3.client("s3", aws_access_key_id=aws_conn.login,
                         aws_secret_access_key=aws_conn.password, region_name="us-east-1")


def _get_snowflake_connection():
    import snowflake.connector
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson
    return snowflake.connector.connect(
        account=extra["account"], user=conn.login, password=conn.password,
        warehouse=extra["warehouse"], database=extra["database"],
        role=extra["role"], schema="GOVERNANCE",
    )


def _slack_alert(text: str):
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        SlackWebhookHook(slack_webhook_conn_id="slack_default").send_text(text)
    except Exception as e:
        logger.error("Slack alert failed: %s", e)

def task_failure_slack_alert(context):
    """Catch-all safety net -- fires for ANY task failure, including
    crashes that never reach an inline _slack_alert() call."""
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

def _verify_s3_file_written(source_name, s3_prefix, uses_date_subfolder=True):
    from datetime import datetime, timezone
    s3_client = _get_s3_client()
    today_date = datetime.now(timezone.utc).date()
    if uses_date_subfolder:
        today_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        full_prefix = f"{s3_prefix.rstrip('/')}/{today_str}/"
        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=full_prefix)
        found = response.get("KeyCount", 0) > 0
    else:
        full_prefix = f"{s3_prefix.rstrip('/')}/"
        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=full_prefix)
        contents = response.get("Contents", [])
        found = any(obj["LastModified"].date() == today_date for obj in contents)
    if not found:
        _slack_alert(f":red_circle: *S3 write verification failed* for {source_name}\nNo file modified today under `{full_prefix}`")
        raise ValueError(f"{source_name}: no file modified today found under {full_prefix}")
    return True


def _invoke_lambda(payload, source_name, s3_prefix, uses_date_subfolder=True):
    client = _get_lambda_client()
    response = client.invoke(FunctionName=LAMBDA_FUNCTION_NAME, InvocationType="RequestResponse",
                              Payload=json.dumps(payload).encode())
    result = json.loads(response["Payload"].read())
    if response.get("StatusCode") != 200 or "errorMessage" in result:
        _slack_alert(f":red_circle: *Lambda invoke failed* for {source_name}\n```{result}```")
        raise ValueError(f"Lambda invocation failed for {source_name}: {result}")
    _verify_s3_file_written(source_name, s3_prefix, uses_date_subfolder=uses_date_subfolder)
    _slack_alert(f":package: *{source_name}* generated and confirmed in S3")
    return result


def _validate_and_load(procedure_name: str, source_name: str):
    conn = _get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"CALL FINTECH_PROD.GOVERNANCE.{procedure_name}();")
        result = cur.fetchone()[0]
        logger.info("%s succeeded: %s", procedure_name, result)

        if "No new files" in result:
            _slack_alert(f":information_source: *{source_name}*: already up to date -- {result}")
        else:
            _slack_alert(f":white_check_mark: *{source_name}* validated and loaded: {result}")

        return result
    except Exception as e:
        logger.error("%s failed: %s", procedure_name, e)
        _slack_alert(f":warning: *{source_name} validation failed*\n```{e}```")
        raise
    finally:
        conn.close()


@dag(
    dag_id="generate_account_activity_dag",
    schedule="0 6 */3 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2),"on_failure_callback": task_failure_slack_alert},
    tags=["ingestion", "fintech", "generation", "phase1", "phase2"],
)
def generate_account_activity_dag():

    @task
    def generate_account_activity():
        return _invoke_lambda(
            {"dataset": "account_activity", "rows": 500, "mode": "update", "account_id_max": 280000, "bucket": BUCKET},
            "account_activity",
            s3_prefix="account_activity",
            uses_date_subfolder=False,
        )

    @task
    def validate_and_load_account_activity(_prev):
        return _validate_and_load("LOAD_AND_VALIDATE_ACCOUNT_ACTIVITY", "account_activity")

    validate_and_load_account_activity(generate_account_activity())


generate_account_activity_dag()