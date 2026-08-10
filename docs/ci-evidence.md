# CI evidence

**How to verify every claim `.github/workflows/ci.yml` makes, on your own machine, in about a
minute:**

```bash
make ci-local
```

That runs each fast-lane check with the same command the corresponding CI job uses, printing
the CI job name beside each result so the mapping is checkable rather than asserted.

```
13 passed, 0 failed, 0 skipped, in 49s
```

---

## Current status of GitHub Actions on this repository

Stated plainly, because a reader will notice the badge is not green.

The workflow is valid and the checks are real, but **no run has executed on GitHub**, for a
reason that has nothing to do with this pipeline. GitHub's own annotation on the most recent
run:

> The job was not started because your account is locked due to a billing issue.

Diagnosis, in the order it was established:

| Step | Finding |
|---|---|
| Every run ends in `startup_failure` with **zero jobs created** | The failure is at run creation, before any step or runner is involved |
| A five-line minimal workflow fails identically | Not the workflow file |
| A second, unrelated repository shows the same failure since 29 June 2026 | Not this repository |
| A self-hosted runner was registered and a run dispatched at it: still `startup_failure` | Not runner availability, and self-hosting does not bypass it |
| The repository was made public, so Actions minutes are unmetered | **7 jobs were created and dispatched**, then failed with the annotation above |

So the block is an account-level billing lock. It is resolvable only in account settings, and
the moment it clears, these runs execute: making the repository public already restored job
creation, which was the part under this repository's control.

`make ci-local` exists because of this, and it is arguably the better artifact: a reviewer can
verify the checks directly rather than trusting a green square produced by someone else's
infrastructure.

## What each CI job validates, and how to run it yourself

| CI job | What it proves | Run it directly |
|---|---|---|
| `lint` | ruff, including the `S` security rules that flag a hardcoded credential or a shell injection | `make lint` |
| `unit` | 59 unit tests against committed API fixtures, no network and no containers. Coverage floor 85 percent on the four modules a no-container lane can reach, currently 90 | `make test` |
| `static` | The compose model resolves across every profile; the Debezium connector config renders to a valid Connect payload with no unresolved placeholders; **`promtool test rules`** unit-tests all 10 alert rules; the Grafana dashboard is valid with every panel documented; the 11-rule project convention gate | `bash scripts/ci/convention_gate.sh` |
| `dags` | Both Airflow DAGs import with no errors, and each has a `one_failed` watcher and a `doc_md` | `docker compose run --rm --entrypoint python airflow /opt/airflow/tests/check_dags.py` |
| `dbt` | `dbt parse` compiles every model, macro, test and YAML file without touching a warehouse, so a Jinja error or a bad `ref` is caught in seconds | `docker compose run --rm --entrypoint dbt pipeline parse --project-dir /app/dbt --profiles-dir /app/dbt` |
| `integration` | The full stack end to end: `make demo` offline, strict per-stage verification, UPDATE and DELETE propagation, a second ingestion writing **nothing**, the incremental model not duplicating, and every Prometheus target up with no alerts firing | `make demo && make verify && make demo-mutations` |
| `ci-summary` | A single required check, so branch protection does not need editing when a job is added | n/a |

## Local run, 11 August 2026

Full output of `make ci-local` on a clean working tree:

| Result | CI job | Check |
|---|---|---|
| PASS | `lint` | ruff check |
| PASS | `lint` | ruff format --check |
| PASS | `unit` | 59 unit tests, coverage floor 85 on the reachable modules |
| PASS | `static` | compose model resolves across every profile |
| PASS | `static` | connector config renders to a valid Connect payload |
| PASS | `static` | alert rules are syntactically valid (promtool check rules) |
| PASS | `static` | alert rules behave as intended (promtool test rules, 9 cases) |
| PASS | `static` | Grafana dashboard valid, every panel documented |
| PASS | `static` | project convention gate (11 rules) |
| PASS | `static` | grep -P PCRE is live, so the em-dash rule can actually fail |
| PASS | `dbt` | dbt parse: every model, macro, test and YAML compiles |
| PASS | `dags` | both DAGs import cleanly, each with a watcher and a doc_md |
| PASS | `workflow` | actionlint on .github/workflows/ci.yml |

13 passed, 0 failed, 49s.

## The integration lane, verified locally

The heavy lane cannot run in 49 seconds, so it is verified separately with the same assertions
the workflow makes. Measured on this stack:

| Assertion | Result |
|---|---|
| Cold start from empty volumes, 4 services healthy | 22 seconds |
| API to PostgreSQL | 2,970 observations, 5 countries, 9 indicators, 66 years |
| PostgreSQL to ClickHouse parity via CDC | 2,970 = 2,970, reached about 3 seconds after ingestion |
| End-to-end CDC lag, measured against the Debezium heartbeat | about 3 seconds |
| Idempotency: a second ingestion | `unchanged=2970`, zero rows rewritten, zero spurious CDC events |
| dbt | 58 tests green, three consecutive builds, fact stable at 2,970 rows |
| Mutation propagation | INSERT, UPDATE and DELETE each reach the marts; the tombstone is retained on disk and hidden from `FINAL` reads |
| Airflow | 11 tasks green, watcher correctly skipped |
| Observability | 4 scrape targets up, 10 alert rules loaded, none firing |

Reproduce with:

```bash
make demo             # about 3 to 6 min warm, or SOURCE_API_MODE=fixture for about 1 second
make verify           # per-stage row counts and measured CDC lag, exit code is meaningful
make demo-mutations   # UPDATE and DELETE propagation
```

## A note on the two checks that exist because they nearly failed silently

Two entries above are unusual, and both earn their place:

**`grep -P PCRE is live`.** The convention gate's em-dash rule uses
`grep -rlP '[\x{2014}\x{2013}]' ... 2>/dev/null || true`. GNU grep only enables PCRE2 UTF mode
in a UTF-8 locale, and that trailing `|| true` swallows the error, so in a bare environment the
rule passes forever without checking anything. A convention rule that cannot fail is not a
rule, so its machinery is asserted directly.

**`promtool test rules`.** Alert rules are code, and untested alert rules fail in the only way
that matters: silently. These tests caught two bugs review had not. `CdcParityBroken` divided
two series carrying different label names, and PromQL matches on the full label set, so the
expression evaluated to an empty vector and the alert **could never have fired** while looking
exactly like one that was passing. And `ServiceDown` set a static `component` label, which
overrides the label from the scraped series, so a ClickHouse outage would have been relabelled
"platform" and lost its only routing information.
