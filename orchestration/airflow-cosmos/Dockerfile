FROM astrocrpublic.azurecr.io/runtime:3.3-2

RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir dbt-snowflake

ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=90