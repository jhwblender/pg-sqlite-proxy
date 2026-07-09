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
| `TIMESTAMP` / `TIMESTAMPTZ` | `TEXT` |
| `SMALLINT` | `INTEGER` |
| `BOOLEAN` / `TRUE` / `FALSE` | `INTEGER` / `1` / `0` |
| `VARCHAR(n)` | `TEXT` |
| `DECIMAL(p,s)` | `REAL` |
| `NOW()` / `version()` | `CURRENT_TIMESTAMP` / a literal version string |
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
| `pg_advisory_xact_lock`, `ALTER TABLE … ADD/DROP CONSTRAINT` | silently ignored |
| `DROP TABLE/VIEW/INDEX … CASCADE \| RESTRICT` | the clause is stripped |
| `CREATE SCHEMA …` | no-op (SQLite has no schema concept) |
| `"public".` / `public.` table qualifiers | stripped |
| `CREATE TYPE … AS ENUM (…)` | registered; the type name becomes `TEXT` wherever used as a column type (values are not validated) |
| `DROP TYPE` / `ALTER TYPE` | no-op |
| bare `OFFSET` with no `LIMIT` | gets a `LIMIT -1` prefix (SQLite requires `OFFSET` to follow a `LIMIT`) |
| leading `-- comment` lines | stripped before translation, so statement-anchored patterns above still match |
| `INSERT/UPDATE/DELETE … RETURNING …` | executed as a query and the returned row(s) sent back, not discarded |
| `BEGIN` / `COMMIT` / `ROLLBACK` | executed for real against the session's SQLite connection — `ROLLBACK` genuinely undoes uncommitted writes |

Both the **simple query** protocol and the **extended query / prepared-statement** protocol are supported.

### Timestamp parameter normalization

A bound `Date` parameter and SQLite's own `CURRENT_TIMESTAMP` can arrive in different text formats — e.g. `node-postgres` serializes JS `Date` values with the *local* timezone offset (`2026-07-08T18:33:02.370-05:00`), while `CURRENT_TIMESTAMP` produces UTC with no offset (`2026-07-08 23:33:02`). Since timestamps are stored as plain `TEXT` and compared as strings, mixing the two sources in one range query could silently return wrong results. Any bound parameter that parses as a timestamp is normalized to UTC, second precision, in the same format `CURRENT_TIMESTAMP` produces, before it's bound.

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
- Column OIDs are inferred from SQLite declared types (following SQLite's own type-affinity substring rules, not an exact match — see `sqliteOID`) and, when a real row is available to inspect, from the actual Go type of its value — so clients like `node-postgres` receive numbers as numbers rather than strings.
- `Describe` (extended query protocol) answers a `SELECT` with its real result-column shape by running the statement once with parameters substituted for `0`, purely to read back column metadata — no row is ever fetched or sent from that probe. This exists because some clients (notably Prisma's Rust-based tooling) treat a mismatch between what `Describe` promises and what `Execute` actually sends as a protocol error, not just a warning.

---

## Prisma migrations

`prisma db push` and `prisma migrate dev` both rely on Prisma's Rust **schema engine**, which introspects the target database's real Postgres system catalog (`pg_namespace`, `pg_class`, `information_schema.*`, …) to compute a diff. This proxy does not — and does not intend to — emulate `pg_catalog`; that's a fundamentally larger undertaking than SQL translation and is out of scope here. Running `db push`/`migrate dev` directly against this proxy will fail once the schema engine reaches its first catalog query.

The Prisma **query engine** (what `PrismaClient` actually uses at runtime, including through the `@prisma/adapter-pg` driver adapter) does *not* need catalog access — ordinary `create`/`findMany`/`update`/`delete`/`$transaction()` calls work normally once the schema exists.

The practical workflow: generate the migration SQL without a live connection, then apply it directly.

```bash
# Generates the CREATE TABLE/TYPE/INDEX + ALTER TABLE ... ADD CONSTRAINT
# SQL for your schema without touching any database.
npx prisma migrate diff --from-empty --to-schema prisma/schema.prisma --script > migration.sql

# Apply it via any pg client pointed at the proxy (psql, a short script
# using `pg`, etc.) — see the project's own tooling for an example.
```

Foreign keys are always emitted by Prisma as a separate `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY` statement (so it can support circular references without knowing the full table graph up front). SQLite cannot add a foreign key to an already-created table via `ALTER TABLE` at all — those statements are accepted as no-ops (see the translation table above), so **relationships from a Prisma-generated migration are not enforced at the database level** against this proxy, even though they work normally against real Postgres. This matches the already-documented `ADD/DROP CONSTRAINT` limitation below.

---

## Known limitations

- **Booleans** are stored as `0`/`1` integers.
- **Timestamps** are stored as text (see "Timestamp parameter normalization" above for how cross-source comparisons stay consistent).
- **Enum values** are not validated — a `CREATE TYPE ... AS ENUM` column becomes plain `TEXT`; any string can be stored.
- **`DISTINCT ON`** is silently stripped — results may contain duplicates if the application relied on it for deduplication.
- **`ADD / DROP CONSTRAINT`** (including foreign keys added via `ALTER TABLE`) is silently ignored — those constraints are not enforced. See "Prisma migrations" above.
- **`prisma db push` / `prisma migrate dev`** cannot run directly against this proxy — see "Prisma migrations" above.
- **Window functions** (`OVER (…)`) are not translated and will produce an error.
- **Single writer** — SQLite allows only one concurrent writer; concurrent writes queue behind a 10 s busy timeout.
- **No authentication** — any credentials are accepted.
- **No TLS.**
- **Not for production** — this is a development / CI tool.

---

## License

MIT
