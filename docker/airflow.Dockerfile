# Airflow image with dbt available, but dbt installed into its own virtualenv.
#
# Why the separate venv: Airflow pins a large, tightly constrained dependency
# set, and so does dbt-core. Installing both into the same interpreter means one
# resolver has to satisfy both, and an Airflow bump then silently drags dbt's
# adapter contract with it. Keeping dbt in /opt/dbt-venv means the two never
# negotiate, and the DAG calls dbt through an absolute path.
FROM apache/airflow:2.10.3

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl postgresql-client \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# The pipeline's own dependencies go into Airflow's interpreter, because the
# ingestion task runs in-process as a PythonOperator. dbt does not.
COPY requirements/pipeline.txt /tmp/pipeline.txt
RUN grep -vE '^(dbt-core|dbt-clickhouse|dbt-adapters)' /tmp/pipeline.txt > /tmp/airflow-side.txt \
 && pip install --no-cache-dir -r /tmp/airflow-side.txt

# dbt, isolated.
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt-venv/bin/pip install --no-cache-dir \
      "dbt-core==1.11.12" "dbt-clickhouse==1.10.1" "dbt-adapters==1.22.10"

# The DAGs reference this rather than a bare `dbt` on PATH, so there is no doubt
# about which interpreter runs.
ENV DBT_BIN=/opt/dbt-venv/bin/dbt \
    DBT_PROJECT_DIR=/opt/airflow/dbt \
    DBT_PROFILES_DIR=/opt/airflow/dbt \
    DBT_TARGET_PATH=/tmp/dbt-target \
    DBT_LOG_PATH=/tmp/dbt-logs \
    PYTHONPATH=/opt/airflow
