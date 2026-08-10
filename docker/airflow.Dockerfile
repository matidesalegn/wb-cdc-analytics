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
 && rm -rf /var/lib/apt/lists/* \
 # Create the venv directory as root and hand it to airflow. The airflow user cannot
 # write to /opt, so building the venv directly as that user fails with a bare
 # "Permission denied: '/opt/dbt-venv'" that does not mention the user at all.
 && mkdir -p /opt/dbt-venv \
 && chown airflow:root /opt/dbt-venv
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

# Resolve dbt packages at build time, exactly as the pipeline image does.
#
# This is easy to forget and fails late: without it the image builds fine, the DAG
# parses fine, and the first dbt task dies with "dbt found 1 package(s) specified in
# packages.yml, but only 0 package(s) installed". That is three green steps followed by
# a failure whose cause is in the Dockerfile.
#
# The project is copied here only so `dbt deps` has something to resolve against. At
# runtime /opt/airflow/dbt is bind-mounted read-only and shadows this copy, which is
# precisely why the packages must land in /opt/dbt-packages (set by
# packages-install-path in dbt_project.yml) rather than inside the project directory.
COPY dbt/ /opt/airflow/dbt/
USER root
# `dbt deps` does not just write into its target directory, it REMOVES and recreates
# it. Removing a directory needs write permission on the PARENT, so owning
# /opt/dbt-packages is not sufficient and the failure is a bare
# "PermissionError: '/opt/dbt-packages'" that points at the child. The airflow user's
# group is root, so granting the group write on /opt is the narrow fix; the alternative
# is chowning /opt wholesale, which is broader than needed.
RUN mkdir -p /opt/dbt-packages \
 && chown -R airflow:root /opt/dbt-packages /opt/airflow/dbt \
 && chmod g+w /opt /opt/dbt-packages
USER airflow
RUN /opt/dbt-venv/bin/dbt deps \
      --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# The DAGs reference this rather than a bare `dbt` on PATH, so there is no doubt
# about which interpreter runs.
ENV DBT_BIN=/opt/dbt-venv/bin/dbt \
    DBT_PROJECT_DIR=/opt/airflow/dbt \
    DBT_PROFILES_DIR=/opt/airflow/dbt \
    DBT_TARGET_PATH=/tmp/dbt-target \
    DBT_LOG_PATH=/tmp/dbt-logs \
    PYTHONPATH=/opt/airflow
