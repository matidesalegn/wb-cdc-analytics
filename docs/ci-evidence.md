# CI evidence

**How to verify every claim `.github/workflows/ci-cd.yml` makes, on your own machine, in about a
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

## Where CI runs on this repository, and how that was established

**Read this before looking at the badge, because the badge will not render.** This repository is
private, and GitHub does not serve the workflow badge or the Actions run history to anyone without
repository access. The runs below are real and were green; they are recorded here with their run
ids and timings so the claim is checkable rather than asserted, and `make ci-local` reproduces
every fast-lane check on your own machine in about a minute without GitHub at all.

**The state of CI, stated plainly.** GitHub-hosted compute is unavailable on this account under a
billing lock. While the repository was public, pointing the workflow at a self-hosted runner made
it fully green, including the integration lane and a real deployment. Making the repository
private stopped runs being created at all, because the lock blocks run creation on private
repositories: a dispatch now returns `startup_failure` with **zero jobs created**. So the honest
summary is that the pipeline is proven and currently cannot execute, for a reason that has nothing
to do with this code.

### The green runs, recorded

| Run | Result | Head | Detail |
|---|---|---|---|
| `31494637176`, 13:08 | **7/7 success** | `cd36f962` | lint 27s, unit 46s, static 27s, dags 27s, dbt 36s, integration 134s, summary 13s |
| `31496160626`, 13:26 | **7/7 success** | | unattended, no manual step |
| `31505852373`, 15:13 | **8/8 success** | `f52a2313` | as above plus **Deploy to demo environment, 35s**, which deployed that SHA to the live host |
| `31515308308`, 17:5x | **8/8 success** | `67fcf52a` | the workflow after renaming to `ci-cd.yml`. lint 25s, unit 50s, static 32s, dags 34s, dbt 38s, integration 145s, **deploy 33s**, summary 12s |

The deployed commit on the demo host is `67fcf52a`, recorded in `~/.wbcdc-deploy/current-sha`
there, with `previous-sha` holding `f52a2313` so a rollback has somewhere to go. Both match the
runs that deployed them, which is the check worth doing: the host is the authority on what is
live, not the workflow's own summary.

The last of those runs was produced by briefly making the repository public, dispatching, and
making it private again, precisely because a private repository under this billing lock creates
no jobs at all. That is worth stating rather than leaving the reader to wonder how a green run
exists for a repository whose Actions page is empty.

### Two operational facts worth stating rather than hiding

**A deployment record is created when the deploy job starts, not when it succeeds.** Run
`31512907295` shows a deployment for its SHA and still failed: the SSH step timed out. Reading the
deployments API as proof of a successful deploy is a mistake, and the SHA recorded on the host is
the authority.

**The delivery lane is fragile by construction here, and the cause is worth naming.** The runner
is a laptop on a residential connection, while the demo host allowlists SSH to specific /32s. When
the ISP rotates the laptop's address, the deploy step fails with a connection timeout even though
nothing about the code or the host has changed. The correct fix is to stop depending on an inbound
port at all, by driving the deploy through AWS Systems Manager Session Manager instead of SSH; the
allowlist is a stopgap. This is recorded because a CD lane whose failure mode is "somebody's home
IP changed" is exactly the sort of thing that should be written down rather than rediscovered.

GitHub's annotation on a hosted job, verbatim:

> The job was not started because your account is locked due to a billing issue.

Diagnosis, in the order it was established. Each row is a measurement, not an inference:

| Step | Finding |
|---|---|
| Every run ended in `startup_failure` with **zero jobs created** | The failure was at run creation, before any step or runner was involved |
| A five-line minimal workflow failed identically | Not the workflow file |
| A second, unrelated repository has failed the same way since 29 June 2026 | Not this repository. The block is account-wide |
| The repository was made public | **7 jobs were now created**, each with `started_at` and `completed_at` 2 seconds apart and an **empty steps array**. Not even `Set up job` ran |
| Public repositories get unlimited free hosted minutes, yet nothing executed | So the obstacle is **not** metered compute. If it were, going public would have fixed it outright |
| A self-hosted runner was registered and a probe workflow dispatched at it | **3 steps executed, conclusion success.** Self-hosted minutes are not billed, so those jobs run |

The conclusion that survived: the lock gates **hosted** compute specifically. Making the
repository public restored job *creation*, which was the necessary precondition, and a
self-hosted runner supplies the *execution*. An earlier self-hosted attempt had failed and
seemed to disprove this, but that test ran while the repository was still private, when no job
was being created for any runner to accept.

`runs-on` is therefore `${{ vars.CI_RUNNER || 'ubuntu-latest' }}` on every job. The default is
unchanged, so a fork gets working hosted CI with no setup; a repository variable redirects the
jobs with no file edit, and reverting is deleting the variable.

### Two latent bugs that only a real machine could expose

Both would fail on a hosted runner too. Neither had ever been caught, because this workflow had
never actually executed on GitHub.

| Bug | Why hosted runners hid it |
|---|---|
| The observability step asserted every Prometheus scrape target was up immediately after `up --wait`. Grafana reported healthy at `13:02:25.37` and the assertion fired at `13:02:25.39`, **20ms later**, against a 15-second scrape interval | It had never run. Locally a human checks seconds or minutes after starting the stack, never inside the same 20ms. Now a bounded wait precedes the assertions, which are left intact so a real failure still names the specific job |
| `dbt_project.yml` hardcoded `packages-install-path` to `/opt/dbt-packages`. The containers need an absolute path outside the read-only project mount, but `/opt` is root-owned on an ordinary Linux box, so `dbt deps` could not create it | A hosted runner's user can write to `/opt`. Now `{{ env_var('DBT_PACKAGES_PATH', '/opt/dbt-packages') }}`, so the container default is unchanged and the CI job points at its per-job temp directory |

A third issue was environmental rather than a bug: the Actions **cache service** is blocked by
the same lock, and `actions/setup-python` does not fail fast on that. It spent ten minutes on a
restore that ended in "Server failed to authenticate the request", which is longer than the lint
job's entire timeout, so jobs were killed and reported as "Canceled" as though a human had
cancelled them. The tell was that `static`, the only fast-lane job without `cache: pip`, was also
the only one finishing quickly. `cache: pip` is now gated to hosted runners.

`make ci-local` remains in the repository regardless, and is arguably still the better artifact:
a reviewer can verify every check directly rather than trusting a green square produced on
someone else's machine.

## What each CI job validates, and how to run it yourself

| CI job | What it proves | Run it directly |
|---|---|---|
| `lint` | ruff, including the `S` security rules that flag a hardcoded credential or a shell injection | `make lint` |
| `unit` | 59 unit tests against committed API fixtures, no network and no containers. Coverage floor 85 percent on the four modules a no-container lane can reach, currently 90 | `make test` |
| `static` | The compose model resolves across every profile; the Debezium connector config renders to a valid Connect payload with no unresolved placeholders; **`promtool test rules`** unit-tests all 10 alert rules across 13 cases; the Grafana dashboard is valid with every panel documented; the 12-rule project convention gate | `bash scripts/ci/convention_gate.sh` |
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
| PASS | `static` | alert rules behave as intended (promtool test rules, 13 cases) |
| PASS | `static` | Grafana dashboard valid, every panel documented |
| PASS | `static` | project convention gate (12 rules) |
| PASS | `static` | grep -P PCRE is live, so the em-dash rule can actually fail |
| PASS | `dbt` | dbt parse: every model, macro, test and YAML compiles |
| PASS | `dags` | both DAGs import cleanly, each with a watcher and a doc_md |
| PASS | `workflow` | actionlint on .github/workflows/ci-cd.yml |

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
make demo             # about 50s warm; see the timing note below for the slow case
make demo-offline     # the same, replaying committed fixtures with no network
make verify           # per-stage row counts and measured CDC lag, exit code is meaningful
make demo-mutations   # UPDATE and DELETE propagation
```

## Clean-clone test, 11 August 2026

Run as a reviewer would: `git clone` of the public repository into an empty directory, with
every container and volume of the previous stack destroyed first, so nothing was inherited.
Images were already cached locally, which is the warm-image case the README quotes.

| Step | Command | Result |
|---|---|---|
| Clone | `git clone https://github.com/matidesalegn/wb-cdc-analytics.git` | 115 files, no `.env`, no `dbt/target`, no volumes |
| One command | `make demo` | **exit 0 in about 50 seconds**, all six stages PASS, 58 dbt tests green |
| CI on GitHub | `git push` to main | **7 jobs, all green.** lint 27s, unit 46s, static 27s, dags 27s, dbt 36s, integration 134s, summary 13s |
| One command, no network | `make demo-offline` | exit 0 in 53 seconds, replaying the committed fixtures |
| Per-stage verification | `make verify` | 2,970 rows in PostgreSQL = 2,970 in ClickHouse = 2,970 in the fact table, 330 feature rows, CDC lag 10s |
| Mutations | `make demo-mutations` | INSERT, UPDATE and DELETE all propagate to the marts; the tombstone is retained on disk and hidden from `FINAL` reads |
| Idempotency | second ingestion | `unchanged=2970`, zero rows rewritten |
| Observability | `docker compose --profile observability up -d --wait` | 4 scrape targets up, 10 alert rules loaded, none firing, Grafana dashboard provisioned and answering a live query with 2970 |
| Orchestration | trigger `wb_cdc_pipeline` | 11 tasks success, watcher correctly skipped |
| Fast lane | `make ci-local` | 13 passed, 0 failed, 66s |
| Teardown | `make down` | Connector removed, `NOTICE: dropped replication slot wb_cdc_slot`, and the slot verified gone |
| Full clean | `make clean` | 0 containers, 0 volumes remaining |

**This test found a real bug, which is why it was worth running.** On the first cold start the
pipeline was completely healthy and `verify_stages.sh` reported FAILURE. ClickHouse creates the
Kafka engine tables before Debezium has created the topics, so each consumer records one
"Broker: Unknown topic or partition" and then reads normally once the topic appears; because
`system.kafka_consumers` keeps exception history, that benign entry persisted. The check had been
filtering on recency, which works on a warm stack and is useless on a cold one, since on a cold
start the benign exception is also seconds old. It now classifies by **recovery** instead: an
unknown-topic exception is benign if that consumer has since read messages, while a consumer that
has read nothing, or any other exception, still fails. The failure the check exists to catch, a
throwing materialized view stalling the consumer, is caught by the parity assertion, which
compares the warehouse against the source rather than trusting the consumer's own view.

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
