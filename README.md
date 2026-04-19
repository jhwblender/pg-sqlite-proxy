# pg-sqlite-proxy

A PostgreSQL wire-protocol proxy that stores data in SQLite. Point any
`libpq`-compatible client at it and it just works — no PostgreSQL installation
required.

Useful for local development, CI environments, and anywhere you want
zero-dependency persistence without the overhead of a full Postgres server.

## How it works

The proxy speaks the PostgreSQL wire protocol (both simple-query and
extended-query / prepared-statement protocols) and translates each query to
SQLite before executing it. Results are returned in the same wire format a real
Postgres server would use, including correct OIDs so numeric types arrive as
numbers rather than strings in clients like node-postgres.

SQL translation covers:

| PostgreSQL | SQLite |
|---|---|
| `$1, $2, …` | `?, ?, …` |
| `SERIAL` / `BIGSERIAL` | `INTEGER` |
| `TIMESTAMPTZ` | `TEXT` |
| `SMALLINT` | `INTEGER` |
| `BOOLEAN` / `TRUE` / `FALSE` | `INTEGER` / `1` / `0` |
| `VARCHAR(n)` | `TEXT` |
| `DECIMAL(p,s)` | `REAL` |
| `NOW()` | `CURRENT_TIMESTAMP` |
| `ILIKE` | `LIKE` |
| `::type` casts | removed |
| `ADD COLUMN IF NOT EXISTS` | `ADD COLUMN` |
| `DISTINCT ON (…)` | stripped |
| `expr - INTERVAL 'N unit'` | `datetime(expr, '-N unit')` |
| `DATE_TRUNC('day', col)` | `date(col)` |
| `setval()` / `nextval()` | `SELECT 1` |
| `= ANY($N)` | `IN (?, ?, …)` |
| `ALTER TABLE … ADD COLUMN a …, ADD COLUMN b …` | split into separate statements |
| `DELETE FROM tbl alias WHERE …` | alias stripped |
| `pg_advisory_xact_lock`, `ADD/DROP CONSTRAINT` | silently ignored |

## Usage

```
go build -o pg-sqlite-proxy .
./pg-sqlite-proxy [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-db` | `data.db` | Path to the SQLite database file |
| `-port` | `5432` | TCP port to listen on |

**Example** — listen on a non-privileged port with a named database file:

```bash
./pg-sqlite-proxy -db myapp.db -port 5433
```

Then point your app at it:

```
DATABASE_URL=postgresql://user:pass@localhost:5433/myapp
```

No password is required; the proxy accepts any credentials without checking them.

## Known limitations

- **Booleans** are stored as `0`/`1` integers, not `true`/`false`.
- **Timestamps** are stored as ISO-8601 text strings (SQLite has no native timestamp type).
- **`DISTINCT ON`** is silently stripped — the result set may contain duplicates if the app relied on it for deduplication.
- **`ADD / DROP CONSTRAINT`** statements are silently ignored — constraints are not enforced.
- **`FILTER (WHERE …)`** on aggregates requires SQLite ≥ 3.44 (bundled automatically via `modernc.org/sqlite`).
- **Window functions** (`OVER (…)`) are not translated and will error if used.
- **Transaction status** — the proxy tracks `BEGIN`/`COMMIT`/`ROLLBACK` to report the correct `ReadyForQuery` status byte, but does not inspect SQLite's internal transaction state.
- **Single writer** — SQLite allows only one concurrent writer; concurrent writes are serialised via a 10 s busy timeout.
- **Not for production** — no authentication, no TLS, no replication.
