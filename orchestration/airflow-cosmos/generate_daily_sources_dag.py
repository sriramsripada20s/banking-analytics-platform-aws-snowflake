"""
PHASE 1 + 2: Replaces EventBridge for the 3 daily batch sources.
Airflow invokes the Lambda, verifies the file landed in S3, then calls
the matching validate-and-load Snowflake procedure.

Validation outcome is determined by whether CALL raises a real
exception, not by parsing the returned string.
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
    return boto3.client(
        "lambda",
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name="us-east-1",
    )


def _get_s3_client():
    import boto3
    aws_conn = BaseHook.get_connection("aws_fintech")
    return boto3.client(
        "s3",
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name="us-east-1",
    )


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
    crashes that never reach an inline _slack_alert() call (worker
    death, OOM, timeout, or an unhandled exception in a code path
    without its own try/except)."""
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

def _verify_s3_file_written(source_name: str, s3_prefix: str, uses_date_subfolder: bool = True):
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
        _slack_alert(
            f":red_circle: *S3 write verification failed* for {source_name}\n"
            f"Lambda reported success but no file modified today found under `{full_prefix}`"
        )
        raise ValueError(
            f"{source_name}: Lambda reported success but no file modified today "
            f"found under {full_prefix}"
        )
    return True


def _invoke_lambda(payload: dict, source_name: str, s3_prefix: str, uses_date_subfolder: bool = True):
    client = _get_lambda_client()
    response = client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )
    result = json.loads(response["Payload"].read())
    status_code = response.get("StatusCode")

    if status_code != 200 or "errorMessage" in result:
        _slack_alert(f":red_circle: *Lambda invoke failed* for {source_name}\n```{result}```")
        raise ValueError(f"Lambda invocation failed for {source_name}: {result}")

    _verify_s3_file_written(source_name, s3_prefix, uses_date_subfolder=uses_date_subfolder)
    _slack_alert(f":package: *{source_name}* generated and confirmed in S3")
    logger.info("%s: Lambda succeeded and S3 write verified", source_name)
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
    dag_id="generate_daily_sources_dag",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, 
                  "retry_delay": timedelta(minutes=2),
                  "on_failure_callback": task_failure_slack_alert
    },
    tags=["ingestion", "fintech", "generation", "phase1", "phase2"],
)
def generate_daily_sources_dag():

    @task
    def generate_settlements():
        return _invoke_lambda(
            {"dataset": "settlements", "rows": 200000, "bucket": BUCKET},
            "settlements", s3_prefix="settlements", uses_date_subfolder=True,
        )

    @task
    def validate_and_load_settlements(_prev):
        return _validate_and_load("LOAD_AND_VALIDATE_SETTLEMENTS", "settlements")

    @task
    def generate_exchange_rates():
        return _invoke_lambda(
            {"dataset": "external_api", "bucket": BUCKET},
            "exchange_rates", s3_prefix="external_api/exchange_rates", uses_date_subfolder=True,
        )

    @task
    def validate_and_load_exchange_rates(_prev):
        return _validate_and_load("LOAD_AND_VALIDATE_EXCHANGE_RATES", "exchange_rates")

    @task
    def generate_support_cases():
        return _invoke_lambda(
            {"dataset": "support_cases", "rows": 5000, "customer_id_max": 200000, "bucket": BUCKET},
            "support_cases", s3_prefix="documents/support_cases", uses_date_subfolder=False,
        )

    @task
    def validate_and_load_support_cases(_prev):
        return _validate_and_load("LOAD_AND_VALIDATE_SUPPORT_CASES", "support_cases")

    validate_and_load_settlements(generate_settlements())
    validate_and_load_exchange_rates(generate_exchange_rates())
    validate_and_load_support_cases(generate_support_cases())


generate_daily_sources_dag()