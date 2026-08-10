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
      # dbt shells out to git for package resolution and reports a failed check in
      # `dbt debug` without it. Cheap to include, and `dbt debug` is the first thing
      # a reviewer runs when the warehouse connection looks wrong.
      git \
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

# dbt writes target/, logs/ and dbt_packages/ relative to the project dir by
# default. All three are redirected outside /app/dbt, for two different reasons:
#
#   target/ and logs/  so a containerised run cannot leave root-owned artifacts in
#                      the developer's working tree, which is a genuinely annoying
#                      thing to clean up.
#   dbt_packages/      because /app/dbt is bind-mounted READ-ONLY at runtime. A
#                      package installed under it at build time would be shadowed by
#                      the mount and invisible, and `dbt deps` at runtime cannot
#                      write there at all. Installing to /opt/dbt-packages at build
#                      time makes the image self-contained: no network call for
#                      packages when the pipeline runs, and CI gets the same
#                      resolved versions the image was built with.
ENV DBT_TARGET_PATH=/tmp/dbt-target \
    DBT_LOG_PATH=/tmp/dbt-logs \
    DBT_PROFILES_DIR=/app/dbt

# Resolve packages at build time, against the copied project.
RUN mkdir -p /opt/dbt-packages \
 && dbt deps --project-dir /app/dbt --profiles-dir /app/dbt \
 && chmod -R a+rX /opt/dbt-packages

# Run as a non-root user. Nothing here needs privileges.
#
# The writable paths have to be chowned explicitly. The dbt deps step above runs as
# root and creates /tmp/dbt-logs on the way, so without this the runtime user cannot
# open its own log file and dbt dies before it does anything, with a PermissionError
# that points at logging rather than at the real cause.
RUN useradd --create-home --uid 10001 pipeline \
 && mkdir -p /tmp/dbt-target /tmp/dbt-logs \
 && chown -R pipeline:pipeline /app /tmp/dbt-target /tmp/dbt-logs /opt/dbt-packages
USER pipeline

CMD ["python", "-m", "ingest.run"]
