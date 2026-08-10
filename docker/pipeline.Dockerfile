# The pipeline runner. One image serves three roles, differentiated by the
# command: the ingestion job, the dbt runner, and the metrics exporter. Keeping
# it to one image keeps the build time and the image count down.
FROM python:3.12-slim

# ca-certificates for the HTTPS call to the public API. postgresql-client and
# curl so the verification scripts can run inside this container rather than
# depending on what the host happens to have installed.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      postgresql-client \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Requirements first so a code change does not invalidate the dependency layer.
COPY requirements/pipeline.txt /tmp/pipeline.txt
RUN pip install --no-cache-dir -r /tmp/pipeline.txt

# Source is bind-mounted in Compose for a fast edit loop, but copied here too so
# the image is self-contained and CI can run it without the mount.
COPY ingest/ /app/ingest/
COPY dbt/ /app/dbt/
COPY scripts/ /app/scripts/

# dbt writes target/ and logs/ relative to the project dir. Give it a writable
# path that is not the bind mount, so a container run cannot leave root-owned
# artifacts in the developer's working tree.
ENV DBT_TARGET_PATH=/tmp/dbt-target \
    DBT_LOG_PATH=/tmp/dbt-logs \
    DBT_PROFILES_DIR=/app/dbt

# Run as a non-root user. Nothing here needs privileges.
RUN useradd --create-home --uid 10001 pipeline \
 && chown -R pipeline:pipeline /app
USER pipeline

CMD ["python", "-m", "ingest.run"]
