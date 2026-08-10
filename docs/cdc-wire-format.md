# CDC wire format, as observed

Every encoding below was read off a live topic, not taken from documentation.
The connector settings that produce it are in
[`cdc/connectors/postgres-source.json`](../cdc/connectors/postgres-source.json):
Debezium 3.6.1, `pgoutput`, JSON converter with `schemas.enable=false`,
`time.precision.mode=connect`, `decimal.handling.mode=double`, and
`ExtractNewRecordState` with `delete.tombstone.handling.mode=rewrite`.

Reproduce it with:

```bash
docker compose exec -T redpanda \
  rpk topic consume wbcdc.wb.observation --offset start --num 3 --format '%v\n'
```

This document exists because guessing these encodings is the most expensive
mistake available in this pipeline. A wrong assumption about one timestamp format
means rewriting every materialized view, and the symptom is not an error, it is a
column of zeroes.

## Observed events

An INSERT, an UPDATE and a DELETE against the same `wb.observation` row:

```json
{"country_id":"ETH","indicator_id":"IC.BUS.NREG","obs_year":2023,"obs_value":12345.6789,
 "obs_decimals":0,"api_last_updated":20635,"source_hash":"oh-v1",
 "ingested_at":"2026-08-10T07:56:18.820714Z","updated_at":"2026-08-10T07:56:18.820714Z",
 "__deleted":"false","__op":"c","__table":"observation","__lsn":26849768,
 "__source_ts_ms":1786348578824}

{... "obs_value":99999.0, "source_hash":"oh-v2",
 "updated_at":"2026-08-10T07:56:18.824893Z", "__deleted":"false","__op":"u",
 "__lsn":26850384 ...}

{"country_id":"ETH","indicator_id":"IC.BUS.NREG","obs_year":2023,"obs_value":null,
 "obs_decimals":null,"api_last_updated":null,"source_hash":"",
 "ingested_at":"1970-01-01T00:00:00.000000Z","updated_at":"1970-01-01T00:00:00.000000Z",
 "__deleted":"true","__op":"d","__table":"observation","__lsn":26850712,
 "__source_ts_ms":1786348578826}
```

## Type mapping

Every ClickHouse expression in the right-hand column was executed against a real
server before being used in a materialized view.

| Postgres type | On the wire | Example | ClickHouse expression |
|---|---|---|---|
| `text` | JSON string | `"ETH"` | `JSONExtractString(payload, 'country_id')` |
| `text` nullable | JSON string or null | `null` | `JSONExtract(payload, 'iso2_code', 'Nullable(String)')` |
| `smallint`, `integer` | JSON number | `2023` | `JSONExtract(payload, 'obs_year', 'Nullable(Int32)')` |
| `double precision` | JSON number, no encoding | `12345.6789` | `JSONExtract(payload, 'obs_value', 'Nullable(Float64)')` |
| `date` | **int32 days since epoch** | `20635` | `toDate(20635)` yields `2026-07-01` |
| `timestamptz` | **ISO-8601 string, microsecond precision** | `"2026-08-10T07:56:18.820714Z"` | `parseDateTime64BestEffortOrNull(s, 6, 'UTC')` |
| `__deleted` | **string, not boolean** | `"false"` / `"true"` | `JSONExtractString(payload,'__deleted') = 'true'` |
| `__op` | string | `c` / `u` / `d` / `r` (`r` = snapshot read) | used as-is |
| `__lsn` | JSON integer | `26849768` | `JSONExtractUInt(payload,'__lsn')` |
| `__source_ts_ms` | int64 epoch millis | `1786348578824` | `fromUnixTimestamp64Milli(n, 'UTC')` |
| `__table` | bare table name, not schema-qualified | `"observation"` | `JSONExtractString(payload,'__table')` |

Three of these would have been wrong if assumed:

- `time.precision.mode=connect` turns `date` into an int32 but leaves
  `timestamptz` as a string, because a Debezium `ZonedTimestamp` is always
  ISO-8601 regardless of the precision mode. The two temporal types in the same
  row therefore arrive in two different shapes.
- `__deleted` is the **string** `"true"`, so `JSONExtractBool` on it returns
  false for a deleted row. That single mistake resurrects every deleted record
  and passes all the tests.
- `toDate` accepts a small integer as days since epoch directly, so no manual
  epoch arithmetic is needed.

## The important asymmetry on DELETE events

With `REPLICA IDENTITY DEFAULT`, Postgres puts only the primary key into the WAL
for a delete. `ExtractNewRecordState` in `rewrite` mode then has to build a full
row from that, and it fills the gaps with **schema defaults, not nulls**:

| Column class | Value on a DELETE event |
|---|---|
| primary key | **correct and complete** (`country_id`, `indicator_id`, `obs_year`) |
| nullable non-key | `null` (`obs_value`, `api_last_updated`) |
| `NOT NULL` text | **empty string `""`**, not null (`source_hash`) |
| `NOT NULL` timestamp | **`"1970-01-01T00:00:00.000000Z"`**, not null (`updated_at`) |

Two consequences the design depends on:

1. **The primary key must be the natural key.** It is the only part of a delete
   event that carries real information. A surrogate sequence key would make a
   delete arrive as an integer with no way to identify the business entity. This
   is why `wb.observation` is keyed on `(country_id, indicator_id, obs_year)`
   rather than on a generated identity column. The alternative,
   `REPLICA IDENTITY FULL`, writes every column of every UPDATE and DELETE into
   the WAL, which is correct but multiplies WAL volume to solve a problem the key
   choice solves for free.

2. **Non-key columns from a delete event must never be trusted.** A delete row
   carries the highest `__lsn` for its key, so any "latest version wins" logic
   picks it, and the row's `source_hash` becomes `""` while its `updated_at`
   becomes 1970. Any model that reads a tombstone's payload reads garbage.

### What FINAL actually does with a tombstone, measured

Worth stating precisely, because the folklore goes both ways and it is easy to
document the opposite of the truth. Measured on ClickHouse 25.8.29 against
`ReplacingMergeTree(_version, _is_deleted)`:

| Query | Result for a deleted key |
|---|---|
| `SELECT ... FROM raw.observation` | 1 row, `_is_deleted = 1`, all non-key columns null or empty |
| `SELECT ... FROM raw.observation FINAL` | **0 rows** |

So `FINAL` **does** apply the `is_deleted` filter and hides the tombstone. What it
does not do is physically remove it: the row stays on disk and any read that omits
`FINAL` still sees it. Physical removal requires `OPTIMIZE ... FINAL CLEANUP` with
`allow_experimental_replacing_merge_with_cleanup`, which is experimental and off
by default.

The staging layer nonetheless filters `_is_deleted = 0` explicitly. That is not
redundancy for its own sake: any read without `FINAL` sees tombstones, and resting
a business-correctness guarantee on an engine-internal behaviour that has moved
between ClickHouse versions is fragile. The filter costs nothing and the failure
it prevents is silent.

## Heartbeat

A `__debezium-heartbeat.wbcdc` topic appears alongside the data topics. That is
`heartbeat.interval.ms` working: the connector writes to `wb.cdc_heartbeat` on a
timer, the write produces an event the connector acknowledges, and the
acknowledgement advances the replication slot's `confirmed_flush_lsn`. Without
it, an idle capture set on a busy cluster leaves the slot pinned and WAL
accumulates behind it until the disk fills, with nothing in the connector looking
wrong.
