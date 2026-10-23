# TimescaleDB
[TimescaleDB](https://www.tigerdata.com/timescaledb) is a PostgreSQL extension for time-series data. It splits a table into time-based chunks (a *hypertable*) and can compress older chunks into a columnar format.

ingestr supports TimescaleDB as both a source and destination. As a source it behaves exactly like [Postgres](./postgres.md); hypertables and compressed chunks are read transparently. As a destination it creates the target table as a hypertable, configures compression, and can compress chunks at the end of each run.

## URI format

```plaintext
timescaledb://<username>:<password>@<host>:<port>/<database-name>?time_column=<column>&chunk_interval=<interval>&segment_by=<columns>&order_by=<expression>&compress_after=<interval>&compress=<mode>
```

`timescale://` is accepted as an alias. All [Postgres](./postgres.md) connection parameters such as `sslmode` work the same way.

The `timescaledb` extension must already be installed in the database (`CREATE EXTENSION timescaledb`). TimescaleDB 2.11 or newer is required. Compression is not available in the Apache-licensed build of the extension, which some managed PostgreSQL services ship.

Destination URI parameters:
- `time_column`: **required**, the column the hypertable is partitioned by. It must be a `timestamp`, `timestamptz` or `date` column.
- `chunk_interval`: optional, the time range each chunk covers, e.g. `1 day` or `6 hours`. Defaults to the TimescaleDB default (7 days). It only applies when the hypertable is created.
- `segment_by`: optional, comma-separated columns to group compressed rows by, e.g. `device_id,tenant_id`.
- `order_by`: optional, the order of rows within compressed segments, e.g. `ts DESC`.
- `compress_after`: optional, compress chunks whose data is older than this interval, e.g. `7 days`. Required with `compress=policy`.
- `compress`: optional, one of:
  - `policy` (the default when `compress_after` is set): add a compression policy so TimescaleDB's background jobs compress chunks older than `compress_after`.
  - `now`: compress chunks older than `compress_after` during the run, and add the policy when `compress_after` is set. Without `compress_after`, every chunk is compressed and no policy is added, which suits one-off migrations of historical data. With the `append` strategy each chunk is compressed as soon as the load has moved past it; a final pass at the end of the run compresses the rest, including chunks that the run left partially compressed.
  - `off` (the default otherwise): don't compress. `segment_by` and `order_by` are still applied as compression settings.
- `migrate_data`: optional, `true` to convert an existing, non-empty plain table into a hypertable. Without it, ingestr refuses to touch such a table.

Interval values use PostgreSQL interval syntax. URL-encode spaces as `+` or `%20`.

## Example

Load a Postgres table into a TimescaleDB hypertable with daily chunks, and compress everything older than a week once the load finishes:

```sh
ingestr ingest \
  --source-uri 'postgresql://admin:secret@source-host:5432/app' \
  --source-table 'public.readings' \
  --dest-uri 'timescaledb://admin:secret@tsdb-host:5432/metrics?time_column=recorded_at&chunk_interval=1+day&segment_by=device_id&order_by=recorded_at+DESC&compress_after=7+days&compress=now' \
  --dest-table 'public.readings' \
  --incremental-strategy merge \
  --incremental-key recorded_at \
  --primary-key device_id,recorded_at
```

## Behavior

- **Primary keys**: TimescaleDB requires every primary key and unique index on a hypertable to include the time column. ingestr checks this before creating the table, so include `time_column` in `--primary-key` when you use `merge`.
- **Existing tables**: an existing hypertable must already be partitioned by `time_column`. Its chunk interval and compression settings are left unchanged; change them directly in the database if needed. An existing plain table is converted only if it is empty or `migrate_data=true` is set.
- **Staging tables**: the staging tables ingestr uses for `merge`, `delete+insert`, `scd2` and `replace` stay plain tables. Only the target becomes a hypertable.
- **Replace**: `replace` empties and reloads the target in place, so the hypertable, its policies and any continuous aggregates built on it are kept.
- **Compression timing**: with `compress=now` and the `append` strategy, a background worker compresses a chunk once every row read so far that could fall into it has been committed, while later chunks are still loading. This works best when the source returns rows in time order (e.g. `--incremental-key` set to `time_column`); a row that arrives after its chunk was compressed is still written correctly, and the final pass recompresses that chunk. `replace`, `merge` and `delete+insert` write the hypertable in a single transaction at the end, so their chunks are compressed one at a time after the data and any CDC state have been committed. If the final pass fails the run reports an error, but the loaded data stays in place. Each `compress_chunk` call briefly locks the chunk it compresses.
- **Policies**: if a compression policy already exists on the table, it is kept as is, even if `compress_after` differs.
- **Schema evolution**: TimescaleDB restricts changing column types and dropping columns on hypertables with compressed chunks; such changes fail with the error TimescaleDB reports.
- **Multi-table runs**: every table must contain `time_column`. With `compress=now`, multi-table `append` runs compress completed chunks during the load but skip the final pass, so the last chunks of each table are left to the compression policy.
