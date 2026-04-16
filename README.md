# Infinite Pixels Database Proxy

SQLite-based PostgreSQL proxy for development without admin rights.

## Setup

1. Build the proxy:
   ```bash
   go build -o pg-proxy.exe
   ```

2. Run the proxy (without elevation):
   ```bash
   .\pg-proxy.exe
   ```

3. Update your `.env` to use port 5433:
   ```
   DATABASE_URL=postgresql://user:pass@localhost:5433/infinite_pixels
   ```

## What it does

The proxy translates PostgreSQL wire protocol to SQLite while adapting syntax differences:

- `$1, $2, ...` → `?, ?, ...` (SQLite parameter syntax)
- `NOW()` → `strftime('%s', 'now')`
- `INTERVAL 'N days'` → Unix timestamp math
- PostgreSQL types → SQLite equivalents (SERIAL→INTEGER, TIMESTAMPTZ→INTEGER, etc.)
- Type casts (`::bigint`) → removed (SQLite is dynamically typed)

## Limitations

- Complex PostgreSQL features (window functions, CTEs, advanced aggregations) may need query adjustments
- Concurrency is limited by SQLite's locking
- Not suitable for production use
