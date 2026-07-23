"""
Production-grade dbt orchestration using Astronomer Cosmos.
"""
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt" / "fintech_project"
SNOWFLAKE_CONN_ID = "snowflake_fintech"
logger = None

import logging
logger = logging.getLogger("airflow.task")

profile_config = ProfileConfig(
    profile_name="fintech_project",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={
            "database": "FINTECH_PROD",
            "schema": "STAGING",
            "warehouse": "DBT_WH",
            "role": "FINTECH_TRANSFORMER",
        },
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/dbt",
)

render_config = RenderConfig(dbt_deps=False)
project_config = ProjectConfig(str(DBT_PROJECT_PATH))


def _extract_dbt_error(log_text: str) -> str:
    clean = re.sub(r"\x1b\[[0-9;]*m", "", log_text)
    hits = []
    for ln in clean.splitlines():
        for pattern, label in [
            (r"ERROR creating.*model\s+([A-Za-z0-9_.]+)", "Model failed to build: {}"),
            (r"Database Error in (?:model|test|snapshot)\s+([A-Za-z0-9_.]+)", "Database error: {}"),
            (r"Compilation Error in (?:model|test|snapshot)\s+([A-Za-z0-9_.]+)", "Compilation error: {}"),
            (r"Failure in test\s+([A-Za-z0-9_.]+)", "Test failed: {}"),
        ]:
            m = re.search(pattern, ln)
            if m:
                hits.append(label.format(m.group(1)))
    seen, unique = set(), []
    for h in hits:
        if h not in seen:
            seen.add(h)
            unique.append(h)
    return "; ".join(unique[:5]) or "No specific dbt error pattern matched -- check the full task log."


def _get_snowflake_connection():
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


def on_failure_callback(context):
    ti = context["task_instance"]
    task_id, dag_id, try_number = ti.task_id, ti.dag_id, ti.try_number
    run_id = context["run_id"]

    log_path = (
        f"/usr/local/airflow/logs/dag_id={dag_id}/run_id={run_id}/"
        f"task_id={task_id}/attempt={try_number}.log"
    )
    if os.path.exists(log_path):
        with open(log_path, "r", errors="ignore") as f:
            root_cause = _extract_dbt_error(f.read())
    else:
        root_cause = "Log file not found -- check Airflow UI directly."

    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        SlackWebhookHook(slack_webhook_conn_id="slack_default").send_text(
            f":red_circle: *{task_id}* failed\n"
            f"*Root cause:* {root_cause}\n"
            f"*Run:* {run_id}"
        )
    except Exception as slack_err:
        logger.error("Slack alert failed: %s", slack_err)

    try:
        conn = _get_snowflake_connection()
        conn.cursor().execute(
            "INSERT INTO FINTECH_PROD.GOVERNANCE.PIPELINE_ERROR_LOG "
            "(dag_id, task_id, status, attempt, error_summary) "
            "VALUES (%s, %s, 'FAILED', %s, %s)",
            (dag_id, task_id, str(try_number), root_cause),
        )
        conn.close()
    except Exception as log_err:
        logger.error("Error-log insert failed: %s", log_err)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_callback,
}

fintech_dbt_pipeline_cosmos = DbtDag(
    dag_id="fintech_dbt_pipeline_cosmos",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    operator_args={"install_deps": False},
    schedule="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=3,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["dbt", "fintech", "cosmos", "production"],
)


def notify_success(**context):
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    SlackWebhookHook(slack_webhook_conn_id="slack_default").send_text(
        f":white_check_mark: Pipeline {context['dag'].dag_id} completed successfully! "
        f"Run: {context['run_id']}"
    )


def notify_pipeline_failed(**context):
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    SlackWebhookHook(slack_webhook_conn_id="slack_default").send_text(
        f":rotating_light: *Pipeline {context['dag'].dag_id} FAILED*\n"
        f"One or more tasks failed -- check individual alerts above for root causes.\n"
        f"Run: {context['run_id']}"
    )


with fintech_dbt_pipeline_cosmos:
    all_existing_tasks = list(fintech_dbt_pipeline_cosmos.tasks)

    notify_success_task = PythonOperator(
        task_id="notify_slack_success",
        python_callable=notify_success,
        trigger_rule="all_success",
    )
    notify_success_task.set_upstream(all_existing_tasks)

    notify_failure_task = PythonOperator(
        task_id="notify_slack_pipeline_failed",
        python_callable=notify_pipeline_failed,
        trigger_rule="one_failed",
    )
    notify_failure_task.set_upstream(all_existing_tasks)