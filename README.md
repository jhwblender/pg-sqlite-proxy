# pg-sqlite-proxy

> Use any PostgreSQL client against a local SQLite file — no Postgres installation required.

`pg-sqlite-proxy` is a lightweight Go binary that speaks the full PostgreSQL wire protocol and stores your data in SQLite. Point `psql`, `node-postgres`, `Prisma`, `Sequelize`, or any `libpq`-compatible driver at it and it just works.

```
your app (pg driver) ──► pg-sqlite-proxy :5432 ──► data.db (SQLite)
```

**Why?** Running a full Postgres server for local dev or CI is heavy. SQLite is a single file, zero config, instant startup. `pg-sqlite-proxy` gives you the familiar pg connection string and protocol while keeping all that simplicity.

---

## Quick start

```bash
go install github.com/jhwblender/pg-sqlite-proxy@latest
pg-sqlite-proxy                      # listens on :5432, writes to data.db
```

Or build from source:

```bash
git clone https://github.com/jhwblender/pg-sqlite-proxy
cd pg-sqlite-proxy
go build -o pg-sqlite-proxy .
./pg-sqlite-proxy -db myapp.db -port 5433
```

Then point your app at it — no password needed:

```
DATABASE_URL=postgresql://user:pass@localhost:5433/myapp
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-db` | `data.db` | Path to the SQLite database file |
| `-port` | `5432` | TCP port to listen on |

---

## What it translates

The proxy rewrites PostgreSQL-flavoured SQL to SQLite-compatible SQL on the fly:

| PostgreSQL | SQLite equivalent |
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

Both the **simple query** protocol and the **extended query / prepared-statement** protocol are supported.

---

## How it works

```
Client                   pg-sqlite-proxy              SQLite
  │                            │                        │
  │── StartupMessage ─────────►│                        │
  │◄─ AuthOk + RFQ ────────────│                        │
  │                            │                        │
  │── Query("SELECT …") ──────►│                        │
  │                            │── translateSQL() ──────►│
  │                            │◄─ rows ────────────────│
  │◄─ RowDescription ──────────│                        │
  │◄─ DataRow × N ─────────────│                        │
  │◄─ CommandComplete + RFQ ───│                        │
```

- One SQLite connection is created per client connection, preserving transaction state across the session.
- WAL mode is enabled for reliable concurrent reads; a 10 s busy timeout serialises concurrent writes.
- Column OIDs are inferred from SQLite declared types and from the actual Go types of the first non-null value in each column, so clients like `node-postgres` receive numbers as numbers rather than strings.

---

## Known limitations

- **Booleans** are stored as `0`/`1` integers.
- **Timestamps** are stored as ISO-8601 text strings.
- **`DISTINCT ON`** is silently stripped — results may contain duplicates if the application relied on it for deduplication.
- **`ADD / DROP CONSTRAINT`** is silently ignored — constraints are not enforced.
- **Window functions** (`OVER (…)`) are not translated and will produce an error.
- **Single writer** — SQLite allows only one concurrent writer; concurrent writes queue behind a 10 s busy timeout.
- **No authentication** — any credentials are accepted.
- **No TLS.**
- **Not for production** — this is a development / CI tool.

---

## License

MIT
