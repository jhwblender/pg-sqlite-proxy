package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	dbs          = make(map[string]*sql.DB)
	dbsMu        sync.Mutex
	singleDBPath string // non-empty: all connections use this one file

	// knownEnumTypes records every `CREATE TYPE <name> AS ENUM (...)` name
	// seen, so later CREATE TABLE column definitions that reference it as a
	// type (e.g. `"recurrenceType" "RecurrenceType" NOT NULL`) can be
	// rewritten to TEXT — SQLite has no user-defined type system at all.
	// Process-global rather than per-connection/per-database: translateSQL
	// has no database handle, and a type created on one connection is
	// visible to every other Postgres client against the same proxy
	// process, matching Postgres's actual (database-wide) scoping closely
	// enough for the single-file-per-proxy-instance case this tool targets.
	// Values are the enum's declared name; existing ENUM values are not
	// tracked or enforced (see the CREATE TYPE comment in translateSQL for
	// why: SQLite has no CHECK-on-arbitrary-values equivalent this proxy
	// implements, so enum columns are plain unconstrained TEXT).
	knownEnumTypes   = make(map[string]bool)
	knownEnumTypesMu sync.Mutex
)

func registerEnumType(name string) {
	knownEnumTypesMu.Lock()
	knownEnumTypes[name] = true
	knownEnumTypesMu.Unlock()
}

func enumTypeNames() []string {
	knownEnumTypesMu.Lock()
	defer knownEnumTypesMu.Unlock()
	names := make([]string, 0, len(knownEnumTypes))
	for n := range knownEnumTypes {
		names = append(names, n)
	}
	return names
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	dbPath := flag.String("db", "", "path to SQLite database file (single-db mode)")
	port   := flag.Int("port", 5432, "TCP port to listen on")
	flag.Parse()

	singleDBPath = *dbPath

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	if singleDBPath != "" {
		log.Printf("pg-proxy listening on :%d (db: %s)", *port, singleDBPath)
	} else {
		log.Printf("pg-proxy listening on :%d (multi-db mode, dbs/ directory)", *port)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn)
	}
}

type prepStmt struct {
	query     string
	paramOIDs []uint32
	// described is true once a Describe('S') for this statement has
	// already sent a real RowDescription (i.e. it's a SELECT — see
	// sendSelectRowDescription). Real Postgres does not send a second,
	// redundant RowDescription from Execute when Describe already
	// provided one for the same statement/portal; unconditionally
	// re-sending it (this proxy's previous behavior) is itself a protocol
	// violation that a strict client rejects outright.
	described bool
}

type portal struct {
	stmt        *prepStmt
	params      []interface{}
	formatCodes []int16
}

func getDB(name string) (*sql.DB, error) {
	dbsMu.Lock()
	defer dbsMu.Unlock()

	key := name
	var path string
	if singleDBPath != "" {
		key = singleDBPath
		path = singleDBPath
	} else {
		if err := os.MkdirAll("dbs", 0755); err != nil {
			return nil, fmt.Errorf("create dir: %w", err)
		}
		path = fmt.Sprintf("dbs/%s.db", name)
	}

	if db, ok := dbs[key]; ok {
		return db, nil
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	// Apply PRAGMAs that the modernc.org/sqlite driver ignores in the DSN.
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("journal_mode WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("foreign_keys ON: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout = 10000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("busy_timeout: %w", err)
	}
	dbs[key] = db
	return db, nil
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	be := pgproto3.NewBackend(conn, conn)
	dbName, err := doStartup(be, conn)
	if err != nil {
		return
	}

	sqlDB, err := getDB(dbName)
	if err != nil {
		sendErr(be, "cannot open database: "+err.Error())
		return
	}

	sqlConn, err := sqlDB.Conn(context.Background())
	if err != nil {
		sendErr(be, "db.Conn: "+err.Error())
		return
	}
	defer sqlConn.Close()

	sqlConn.ExecContext(context.Background(), "PRAGMA busy_timeout = 10000")

	stmts   := map[string]*prepStmt{}
	portals := map[string]*portal{}
	inTx    := false

	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}
		if os.Getenv("PGPROXY_TRACE") != "" {
			log.Printf("TRACE recv: %T %+v", msg, msg)
		}
		switch m := msg.(type) {
		case *pgproto3.Terminate:
			return

		case *pgproto3.Query:
			inTx = simpleQuery(be, sqlConn, m.String, inTx)

		case *pgproto3.Parse:
			paramOIDs := m.ParameterOIDs
			// A client is allowed to Parse without specifying parameter
			// types at all (empty ParameterOIDs) and let the server infer
			// them. This proxy has no type inference, but it must still
			// report the correct parameter *count* in ParameterDescription
			// — echoing back an empty list when the query text actually
			// has $1..$N placeholders previously told strict clients (e.g.
			// Prisma's Rust schema engine) "this statement takes 0
			// parameters" for a statement that plainly takes N, which they
			// correctly treated as a protocol error ("Incorrect number of
			// parameters given to a statement. Expected 0: got: 1").
			// Default every inferred parameter to OID 25 (text) — SQLite
			// is untyped/dynamically-typed per value anyway, so this is
			// never worse than a guess and lets bound values of any type
			// through.
			if len(paramOIDs) == 0 {
				if n := countParamPlaceholders(m.Query); n > 0 {
					paramOIDs = make([]uint32, n)
					for i := range paramOIDs {
						paramOIDs[i] = 25
					}
				}
			}
			stmts[m.Name] = &prepStmt{
				query:     translateSQL(m.Query),
				paramOIDs: paramOIDs,
			}
			be.Send(&pgproto3.ParseComplete{})

		case *pgproto3.Bind:
			stmt, ok := stmts[m.PreparedStatement]
			if !ok {
				sendErr(be, "unknown prepared statement: "+m.PreparedStatement)
				rfq(be, txStatus(inTx))
				continue
			}
			portals[m.DestinationPortal] = &portal{
				stmt:        stmt,
				params:      decodeParams(stmt.paramOIDs, m.ParameterFormatCodes, m.Parameters),
				formatCodes: m.ParameterFormatCodes,
			}
			be.Send(&pgproto3.BindComplete{})

		case *pgproto3.Describe:
			var describedQuery string
			var targetStmt *prepStmt
			if m.ObjectType == 'S' {
				stmt, ok := stmts[m.Name]
				if !ok {
					sendErr(be, "unknown statement: "+m.Name)
					be.Flush()
					continue
				}
				be.Send(&pgproto3.ParameterDescription{ParameterOIDs: stmt.paramOIDs})
				describedQuery = stmt.query
				targetStmt = stmt
			} else if m.ObjectType == 'P' {
				if p, ok := portals[m.Name]; ok {
					describedQuery = p.stmt.query
					targetStmt = p.stmt
				}
			}

			// Real Postgres answers Describe with the statement's actual
			// result-column shape, determined statically (without
			// executing, since a statement-level Describe happens right
			// after Parse — before any parameter values are bound). A
			// lenient client like node-postgres doesn't care if this
			// disagrees with what Execute later sends, but a strict one
			// (e.g. Prisma's Rust schema engine, used by `db push` /
			// `migrate dev`) does: unconditionally answering NoData here
			// and then sending a real RowDescription from Execute for any
			// SELECT is a protocol violation that desyncs its state
			// machine — surfacing as an opaque "unexpected message from
			// server" with no indication Describe was the culprit.
			//
			// This proxy has no query planner to determine column shape
			// without running the statement, so for SELECT specifically it
			// runs the query with NULL-substituted parameters purely to
			// read back column metadata (Columns()/ColumnTypes()), without
			// ever calling Next() to fetch a row. That's an acceptable
			// local-dev tradeoff (see the README's documented scope) as
			// long as it's never done for a RETURNING statement, where
			// running it here would double any side effects — those still
			// get NoData, matching their previous (already-working,
			// verified) behavior via node-postgres/Prisma Client at
			// runtime.
			if isSelect(describedQuery) {
				ok := sendSelectRowDescription(be, sqlConn, describedQuery)
				// Only mark the statement as described when a real
				// RowDescription was actually sent. Marking it
				// unconditionally here (even when the probe failed and
				// NoData was sent instead) previously made Execute *also*
				// skip its own RowDescription — the client then received
				// bare DataRow messages with no column metadata at all,
				// which is worse than the original double-RowDescription
				// bug this was meant to fix.
				if ok && targetStmt != nil {
					targetStmt.described = true
				}
			} else {
				be.Send(&pgproto3.NoData{})
			}
			be.Flush()

		case *pgproto3.Execute:
			p, ok := portals[m.Portal]
			if !ok {
				sendErr(be, "unknown portal: "+m.Portal)
				rfq(be, txStatus(inTx))
				continue
			}
			inTx = execPortal(be, sqlConn, p, inTx)

		case *pgproto3.Sync:
			rfq(be, txStatus(inTx))
			be.Flush()

		case *pgproto3.Close:
			// A well-behaved extended-protocol client closes prepared
			// statements/portals it's done with and waits for
			// CloseComplete before proceeding — the wire protocol
			// requires a reply to every Close. This proxy previously had
			// no case for it at all, so it fell through the switch
			// silently: no reply was ever sent. node-postgres rarely
			// issues explicit Close messages so this went unnoticed, but
			// a stricter client (e.g. Prisma's Rust schema engine, used
			// by `db push`/`migrate dev`) does, and the missing reply
			// desyncs its protocol state machine — the next unrelated
			// response arrives while it's still waiting for
			// CloseComplete, surfacing as "unexpected message from
			// server" with no indication of what actually went wrong.
			switch m.ObjectType {
			case 'S':
				delete(stmts, m.Name)
			case 'P':
				delete(portals, m.Name)
			}
			be.Send(&pgproto3.CloseComplete{})

		default:
			log.Printf("unhandled frontend message type %T — no reply sent, client may desync", m)
		}
		be.Flush()
	}
}

func doStartup(be *pgproto3.Backend, conn net.Conn) (string, error) {
	var msg pgproto3.FrontendMessage
	for {
		var err error
		msg, err = be.ReceiveStartupMessage()
		if err != nil {
			return "", err
		}
		switch msg.(type) {
		case *pgproto3.SSLRequest:
			conn.Write([]byte("N"))
		default:
			goto done
		}
	}
done:
	sm, ok := msg.(*pgproto3.StartupMessage)
	if !ok {
		return "", fmt.Errorf("unexpected startup message %T", msg)
	}

	dbName := sm.Parameters["database"]
	log.Printf("connect: user=%s db=%s", sm.Parameters["user"], dbName)

	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"})
	be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	be.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: uint32(os.Getpid()), SecretKey: []byte{0, 0, 0, 0}})
	rfq(be, 'I')
	be.Flush()
	return dbName, nil
}

// simpleQuery handles the Query message (simple protocol, may be multi-statement).
// Returns the updated inTx state.
func simpleQuery(be *pgproto3.Backend, sqlConn *sql.Conn, raw string, inTx bool) bool {
	for _, s := range splitStatements(raw) {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		u := strings.ToUpper(strings.TrimSpace(s))
		if txCmd, ok := txControlCommand(u); ok {
			var failed bool
			inTx, failed = execTxControl(be, sqlConn, txCmd, inTx)
			if failed {
				break
			}
			continue
		}

		var failed bool
		for _, t := range expandStatement(translateSQL(s)) {
			if isNoOp(t) {
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
				continue
			}
			if isSelect(t) {
				failed = runSelect(be, sqlConn, t, nil, true)
			} else if hasReturning(t) {
				failed = runReturning(be, sqlConn, t, nil)
			} else {
				failed = runDML(be, sqlConn, t, nil)
			}
			if failed {
				break
			}
		}
		if failed {
			break
		}
	}
	rfq(be, txStatus(inTx))
	be.Flush()
	return inTx
}

func execPortal(be *pgproto3.Backend, sqlConn *sql.Conn, p *portal, inTx bool) bool {
	q, args := expandParams(p.stmt.query, p.params)
	if os.Getenv("PGPROXY_TRACE") != "" {
		log.Printf("TRACE execute: query=%q rawQuery=%q params=%v args=%v described=%v", q, p.stmt.query, p.params, args, p.stmt.described)
	}

	u := strings.ToUpper(strings.TrimSpace(q))
	if txCmd, ok := txControlCommand(u); ok {
		inTx, _ = execTxControl(be, sqlConn, txCmd, inTx)
		return inTx
	}

	if isNoOp(q) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
		return inTx
	}
	if isSelect(q) {
		// If Describe already sent a real RowDescription for this
		// statement, don't send a second one here — see the `described`
		// field's doc comment on prepStmt for why that's a protocol
		// violation for strict clients.
		runSelect(be, sqlConn, q, args, !p.stmt.described)
	} else if hasReturning(q) {
		runReturning(be, sqlConn, q, args)
	} else {
		runDML(be, sqlConn, q, args)
	}
	return inTx
}

// txControlCommand recognizes BEGIN/COMMIT/ROLLBACK and returns the literal
// SQLite command to execute plus the correct wire CommandComplete tag.
func txControlCommand(upperTrimmed string) (cmd string, ok bool) {
	switch {
	case strings.HasPrefix(upperTrimmed, "BEGIN") || strings.HasPrefix(upperTrimmed, "START TRANSACTION"):
		return "BEGIN", true
	case strings.HasPrefix(upperTrimmed, "COMMIT") || strings.HasPrefix(upperTrimmed, "END"):
		return "COMMIT", true
	case strings.HasPrefix(upperTrimmed, "ROLLBACK"):
		return "ROLLBACK", true
	}
	return "", false
}

// execTxControl actually executes BEGIN/COMMIT/ROLLBACK against the
// underlying SQLite connection.
//
// Previously these were treated as pure no-ops: the proxy tracked an inTx
// bool purely to report the correct ReadyForQuery transaction-status byte
// to the client, but never issued the real SQL — every statement ran in
// SQLite's own autocommit mode regardless of BEGIN/COMMIT/ROLLBACK. That
// meant a client-side ROLLBACK (e.g. Prisma's `$transaction([...])` on
// failure) never actually undid anything: every write inside the "aborted"
// transaction had already been committed to SQLite the instant it ran. For
// a financial application, that's silent data corruption on every failed
// multi-statement transaction, not just a missing feature.
func execTxControl(be *pgproto3.Backend, sqlConn *sql.Conn, cmd string, inTx bool) (newInTx bool, failed bool) {
	if _, err := sqlConn.ExecContext(context.Background(), cmd); err != nil {
		log.Printf("tx control error: %v | cmd: %s", err, cmd)
		sendErr(be, err.Error())
		return inTx, true
	}
	be.Send(&pgproto3.CommandComplete{CommandTag: []byte(cmd)})
	switch cmd {
	case "BEGIN":
		return true, false
	case "COMMIT", "ROLLBACK":
		return false, false
	}
	return inTx, false
}

// runSelect executes a real SELECT.
// reParamPlaceholder matches a $N parameter placeholder for the
// NULL-substitution done by sendSelectRowDescription, and for counting
// actual parameters in countParamPlaceholders.
var reParamPlaceholder = regexp.MustCompile(`\$(\d+)`)

// stripEnumCasts removes Prisma's CAST($N::text AS "schema"."EnumType")
// wrappers. Prisma uses these to pass enum values over the wire as text;
// because SQLite has no enum types we store the labels as TEXT and the cast
// wrapper would otherwise be evaluated (incorrectly) by SQLite's CAST.
//
// Since the proxy may restart after CREATE TYPE statements have already been
// executed (and knownEnumTypes is empty), we strip ALL casts to quoted
// identifiers with optional schema qualification — not just known enums.
func stripEnumCasts(q string) string {
	// First: strip any cast to a quoted type name (with optional schema prefix).
	// This catches Prisma's `CAST($N::text AS "public"."AccountType")`.
	reSchemaCast := regexp.MustCompile(`(?i)CAST\s*\(\s*([^()]*)\s+AS\s+(?:"[^"]*"\.)*"[^"]*"\s*\)`)
	q = reSchemaCast.ReplaceAllString(q, "$1")

	// Second: also strip casts to known enum types (bare names, for backward
	// compatibility and cases where the type is unquoted).
	for _, name := range enumTypeNames() {
		pattern := fmt.Sprintf(`(?i)CAST\s*\(\s*([^()]*)\s+AS\s+(?:"[^"]*"\.)*(?:"%s"|%s)\s*\)`, regexp.QuoteMeta(name), regexp.QuoteMeta(name))
		re := regexp.MustCompile(pattern)
		q = re.ReplaceAllString(q, "$1")
	}
	return q
}

// countParamPlaceholders returns the highest $N referenced in q (0 if
// none), i.e. the number of parameters the statement actually needs.
func countParamPlaceholders(q string) int {
	max := 0
	for _, m := range reParamPlaceholder.FindAllStringSubmatch(q, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil && n > max {
			max = n
		}
	}
	return max
}

// sendSelectRowDescription answers a Describe for a SELECT statement with
// its actual column shape, by running the query with every $N parameter
// substituted for the literal 0 and reading back Columns()/ColumnTypes() —
// without ever calling Next(), so no row is fetched and no data is sent.
// 0 (rather than NULL) is used because SQLite requires LIMIT/OFFSET to be
// a genuinely numeric expression and errors ("datatype mismatch") on
// `LIMIT NULL` — a shape Prisma emits on essentially every findMany/
// findUnique query. 0 is valid in any context (numeric or coerced-to-text
// comparison) and, for a WHERE-clause comparison, is exactly as harmless
// as NULL would have been: this probe only reads column metadata and
// never calls Next(), so it never returns/inspects an actual row either
// way — what matters is that this doesn't error.
//
// If anything goes wrong (translation produced something that doesn't
// actually run, e.g. a dynamic pattern this proxy doesn't fully support),
// this falls back to NoData rather than failing the Describe outright —
// Execute will still run the real, fully-parameterized query and report
// the authoritative result, this is purely an advisory answer for clients
// strict enough to check it up front. Returns whether a real
// RowDescription was actually sent, so the caller knows whether it's safe
// to skip Execute's own RowDescription later (see prepStmt.described).
func sendSelectRowDescription(be *pgproto3.Backend, sqlConn *sql.Conn, q string) bool {
	probeQuery := reParamPlaceholder.ReplaceAllString(q, "0")

	if os.Getenv("PGPROXY_TRACE") != "" {
		log.Printf("TRACE describe-probe: %q -> %q", q, probeQuery)
	}

	rows, err := sqlConn.QueryContext(context.Background(), probeQuery)
	if err != nil {
		if os.Getenv("PGPROXY_TRACE") != "" {
			log.Printf("TRACE describe-probe error: %v", err)
		}
		be.Send(&pgproto3.NoData{})
		return false
	}
	defer rows.Close()

	rawCols, _ := rows.Columns()
	cols := normalizeCols(rawCols)
	colTypes, _ := rows.ColumnTypes()

	// Peek at one real row if the 0-substituted probe happens to match
	// one (common — e.g. WHERE "userId" = 0 matches nothing, but a plain
	// findMany-style query with no filter matches everything). This is
	// exactly the same runtime-value OID fallback runQueryAndSendRows
	// uses for a real Execute; doing it here too means Describe and
	// Execute infer the *same* OIDs for a column whose declared SQLite
	// type is ambiguous (e.g. a DOUBLE PRECISION column, which SQLite
	// reports as an untyped/empty declared type for aggregate-like
	// expressions). Without this, Describe fell back to a weaker
	// declared-type-only guess than Execute's — and once Execute started
	// trusting Describe's answer (skipping its own RowDescription), that
	// weaker guess leaked to the client as the final word, silently
	// turning numeric columns into strings.
	var peeked []interface{}
	if rows.Next() {
		ptrs := make([]interface{}, len(cols))
		peeked = make([]interface{}, len(cols))
		for i := range peeked {
			ptrs[i] = &peeked[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			peeked = nil
		}
	}

	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, c := range cols {
		oid := uint32(25)
		if i < len(colTypes) {
			oid = sqliteOID(colTypes[i].DatabaseTypeName())
		}
		if (oid == 25 || oid == 17) && peeked != nil && peeked[i] != nil {
			switch peeked[i].(type) {
			case int64:
				oid = 23
			case float64:
				oid = 701
			}
		}
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(c),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          oid,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}
	}
	be.Send(&pgproto3.RowDescription{Fields: fields})
	return true
}

func runSelect(be *pgproto3.Backend, sqlConn *sql.Conn, q string, args []interface{}, sendHeader bool) (failed bool) {
	return runQueryAndSendRows(be, sqlConn, q, args, sendHeader, "SELECT")
}

// runReturning executes an INSERT/UPDATE/DELETE ... RETURNING statement.
//
// Previously every non-SELECT statement — including one with a RETURNING
// clause — went through runDML, which calls ExecContext and discards any
// result set. RETURNING is exactly what Prisma's query engine uses by
// default for `create()` and `update()` (`INSERT ... RETURNING "id", ...`),
// so the write itself succeeded silently while the caller got back zero
// rows instead of the created/updated record, with no error at all. This
// routes RETURNING statements through the same query path as SELECT, just
// reporting a Postgres-shaped verb+count tag instead of "SELECT".
func runReturning(be *pgproto3.Backend, sqlConn *sql.Conn, q string, args []interface{}) (failed bool) {
	u := strings.ToUpper(strings.TrimSpace(q))
	verb := "UPDATE"
	switch {
	case strings.HasPrefix(u, "INSERT"):
		verb = "INSERT"
	case strings.HasPrefix(u, "DELETE"):
		verb = "DELETE"
	}
	return runQueryAndSendRows(be, sqlConn, q, args, true, verb)
}

// runQueryAndSendRows executes a statement expected to return rows — a real
// SELECT, or an INSERT/UPDATE/DELETE ... RETURNING — buffers the result,
// infers each column's wire OID, and sends RowDescription + DataRow(s) +
// CommandComplete. verb is "SELECT", "INSERT", "UPDATE", or "DELETE"; the
// CommandComplete tag is built from it (with a row count for the DML verbs,
// matching Postgres's own tag shape — "INSERT 0 <n>" / "UPDATE <n>" /
// "DELETE <n>" — so RETURNING-based writes don't masquerade as a SELECT to
// the client).
func runQueryAndSendRows(be *pgproto3.Backend, sqlConn *sql.Conn, q string, args []interface{}, sendHeader bool, verb string) (failed bool) {
	rows, err := sqlConn.QueryContext(context.Background(), q, args...)
	if err != nil {
		log.Printf("query error: %v | sql: %s | args: %v", err, q, args)
		sendErr(be, err.Error())
		return true
	}
	defer rows.Close()

	rawCols, _ := rows.Columns()
	cols := normalizeCols(rawCols)
	colTypes, _ := rows.ColumnTypes()

	// Buffer all rows so we can infer OIDs from actual Go types before sending
	// RowDescription. SQLite returns empty type names for aggregate expressions
	// (MIN, MAX, COUNT, etc.) so we can't rely on ColumnTypes alone.
	ptrs := make([]interface{}, len(cols))
	tmp  := make([]interface{}, len(cols))
	for i := range tmp {
		ptrs[i] = &tmp[i]
	}
	var buffered [][]interface{}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			log.Printf("scan error: %v", err)
			sendErr(be, err.Error())
			return true
		}
		row := make([]interface{}, len(cols))
		copy(row, tmp)
		// modernc.org/sqlite sometimes returns []byte for aggregate results.
		// Convert numeric-looking []byte to int64 so downstream formatting works.
		for i, v := range row {
			if b, ok := v.([]byte); ok {
				s := string(b)
				if n, err := strconv.ParseInt(s, 10, 64); err == nil {
					row[i] = n
				}
			}
		}
		buffered = append(buffered, row)
	}
	if err := rows.Err(); err != nil {
		log.Printf("rows error: %v", err)
		sendErr(be, err.Error())
		return true
	}

	// Determine OID per column: prefer declared type, fall back to Go type of first non-nil value.
	oids := make([]uint32, len(cols))
	for i := range cols {
		oid := uint32(25)
		if i < len(colTypes) {
			oid = sqliteOID(colTypes[i].DatabaseTypeName())
		}
		// modernc.org/sqlite returns empty type names for aggregates, so
		// sqliteOID falls back to either 25 (text) or 17 (bytea).  In both
		// cases we must inspect the actual Go value and upgrade to int4 or
		// float8 when the data is numeric, otherwise node-postgres leaves
		// the value as a raw Buffer.
		if oid == 25 || oid == 17 {
			for _, row := range buffered {
				if row[i] == nil {
					continue
				}
				switch v := row[i].(type) {
				case int64:
					oid = 23 // int4 — node-postgres returns JS number (not bigint string)
				case float64:
					oid = 701 // float8
				case []byte:
					if _, err := strconv.ParseInt(string(v), 10, 64); err == nil {
						oid = 23 // int4
					}
				}
				break
			}
		}
		oids[i] = oid
	}

	if sendHeader {
		fields := make([]pgproto3.FieldDescription, len(cols))
		for i, c := range cols {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(c),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          oids[i],
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			}
		}
		be.Send(&pgproto3.RowDescription{Fields: fields})
	}

	for _, row := range buffered {
		vals := make([][]byte, len(cols))
		for i, v := range row {
			if v == nil {
				vals[i] = nil // PostgreSQL NULL
			} else {
				vals[i] = []byte(fmt.Sprint(v))
			}
		}
		be.Send(&pgproto3.DataRow{Values: vals})
	}

	var tag string
	switch verb {
	case "INSERT":
		tag = fmt.Sprintf("INSERT 0 %d", len(buffered))
	case "UPDATE", "DELETE":
		tag = fmt.Sprintf("%s %d", verb, len(buffered))
	default:
		tag = "SELECT "
	}
	be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	return false
}

func runDML(be *pgproto3.Backend, sqlConn *sql.Conn, q string, args []interface{}) bool {
	res, err := sqlConn.ExecContext(context.Background(), q, args...)
	if err != nil {
		log.Printf("exec error: %v | sql: %s", err, q)
		sendErr(be, err.Error())
		return true
	}

	// Determine the verb so we can build a Postgres-shaped CommandComplete tag.
	u := strings.ToUpper(strings.TrimSpace(q))
	verb := "UPDATE"
	switch {
	case strings.HasPrefix(u, "INSERT"):
		verb = "INSERT"
	case strings.HasPrefix(u, "DELETE"):
		verb = "DELETE"
	}

	// Build tag matching Postgres CommandComplete format so node-postgres
	// parses the row count correctly.
	tag := "OK"
	if ra, err := res.RowsAffected(); err == nil {
		switch verb {
		case "INSERT":
			tag = fmt.Sprintf("INSERT 0 %d", ra)
		case "UPDATE", "DELETE":
			tag = fmt.Sprintf("%s %d", verb, ra)
		default:
			tag = fmt.Sprintf("%d", ra)
		}
	}
	be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	return false
}

func rfq(be *pgproto3.Backend, status byte) {
	be.Send(&pgproto3.ReadyForQuery{TxStatus: status})
}

func txStatus(inTx bool) byte {
	if inTx {
		return 'T'
	}
	return 'I'
}

func sendErr(be *pgproto3.Backend, msg string) {
	be.Send(&pgproto3.ErrorResponse{Message: msg})
}

// sqliteOID maps SQLite declared type names to PostgreSQL OIDs.
// sqliteOID maps a declared SQLite column type to a wire OID, following
// SQLite's own documented type-affinity determination rules (substring
// matching, checked in this exact order — see
// https://www.sqlite.org/datatype3.html#determination_of_column_affinity),
// not an exact-string match. This matters because DDL translation passes
// Postgres type names like "DOUBLE PRECISION" through to SQLite mostly
// unchanged (SQLite resolves its own REAL affinity for it via the "DOUB"
// substring rule), but an exact `switch` on the declared type string
// previously didn't recognize "DOUBLE PRECISION" at all — every DOUBLE
// PRECISION column (Prisma's mapping for Float, used throughout
// LivingImpactBudget) silently fell through to being reported as TEXT.
func sqliteOID(declared string) uint32 {
	u := strings.ToUpper(declared)
	switch {
	case strings.Contains(u, "INT"):
		return 23 // int4
	case strings.Contains(u, "CHAR"), strings.Contains(u, "CLOB"), strings.Contains(u, "TEXT"):
		return 25 // text
	case strings.Contains(u, "BLOB"), u == "":
		return 17 // bytea
	case strings.Contains(u, "REAL"), strings.Contains(u, "FLOA"), strings.Contains(u, "DOUB"):
		return 701 // float8
	default:
		// SQLite's NUMERIC affinity catch-all (DECIMAL, NUMERIC, BOOLEAN,
		// DATE, etc.) can hold an int, real, or text value depending on
		// what was actually inserted — there's no single correct static
		// OID for it. Default to text (as before) and let the
		// runtime-value fallback in runQueryAndSendRows /
		// sendSelectRowDescription upgrade it to int4/float8 when an
		// actual row is available to inspect.
		return 25
	}
}

func isNoOp(q string) bool {
	u := strings.ToUpper(strings.TrimSpace(q))
	// BEGIN/COMMIT/ROLLBACK are handled earlier by txControlCommand — they
	// must actually execute, not be no-op'd, so they're intentionally not
	// listed here (see execTxControl's comment for why).
	return u == "" ||
		strings.HasPrefix(u, "SET ") ||
		strings.HasPrefix(u, "SHOW ")
}

func isSelect(q string) bool {
	u := strings.ToUpper(strings.TrimSpace(q))
	return strings.HasPrefix(u, "SELECT")
}

// hasReturning reports whether a non-SELECT statement carries a RETURNING
// clause (INSERT/UPDATE/DELETE ... RETURNING ...) and so must go through
// the query path (runReturning) rather than plain ExecContext (runDML),
// which discards any result set.
var reReturning = regexp.MustCompile(`(?i)\bRETURNING\b`)

func hasReturning(q string) bool {
	return reReturning.MatchString(q)
}

// normalizeCols rewrites fully-qualified column references to bare names,
// and cleans aggregate expressions (count(*), sum(x), etc.) to match
// real PostgreSQL behaviour where unaliased aggregates return just the
// function name.
func normalizeCols(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		// If this is an aggregate/function expression, clean it first
		// (must happen before stripping table prefixes, otherwise the
		// prefix stripper would mangle e.g. COUNT(DISTINCT r.x) → x)).
		if cleaned := cleanAggregateName(c); cleaned != c {
			out[i] = cleaned
			continue
		}
		// Otherwise strip table prefix from bare column references.
		idx := strings.LastIndex(c, ".")
		if idx >= 0 {
			out[i] = c[idx+1:]
		} else {
			out[i] = c
		}
	}
	return out
}

// cleanAggregateName strips the argument list from aggregate function
// column names so they match PostgreSQL.  e.g.
//   "count(*)"           → "count"
//   "count(distinct x)"  → "count"
//   "sum(price)"         → "sum"
func cleanAggregateName(c string) string {
	lc := strings.ToLower(c)
	open := strings.Index(lc, "(")
	if open < 0 {
		return c
	}
	close := strings.LastIndex(lc, ")")
	if close < 0 || close <= open {
		return c
	}
	funcName := lc[:open]
	switch funcName {
	case "count", "sum", "avg", "min", "max",
		"stddev", "variance", "bool_and", "bool_or",
		"string_agg", "array_agg", "json_agg", "jsonb_agg":
		return funcName
	}
	return c
}

var (
	// reInterval: e.g. CURRENT_TIMESTAMP - INTERVAL '24 hour' or col - INTERVAL '1 day'
	reInterval = regexp.MustCompile(`(?i)([\w.]+)\s+-\s+INTERVAL\s+'(\d+)\s+(\w+)'`)

	reDateTrunc = regexp.MustCompile(`(?i)DATE_TRUNC\s*\(\s*'([^']+)'\s*,\s*([^)]+)\s*\)`)

	reMultiAddCol   = regexp.MustCompile(`(?i)^(ALTER\s+TABLE\s+\w+\s+ADD\s+COLUMN\s+.+)`)
	reAddColSplit   = regexp.MustCompile(`(?i),\s*ADD\s+COLUMN\s+`)
	// Scoped specifically to `ADD COLUMN IF NOT EXISTS` — SQLite has no
	// IF NOT EXISTS clause for ADD COLUMN, so it must be stripped there.
	// This used to be a bare `IF NOT EXISTS` match replaced with the
	// literal string "ADD COLUMN", which corrupted every *other* use of
	// IF NOT EXISTS in a query — most importantly `CREATE TABLE IF NOT
	// EXISTS "X" (...)`, which SQLite supports natively and needs no
	// translation at all, became the nonsensical `CREATE TABLE ADD COLUMN
	// "X" (...)` and failed with a syntax error on every idempotent-DDL
	// call (e.g. `prisma db push`, or a table-existence guard in app code).
	reAddColIfNot   = regexp.MustCompile(`(?i)\bADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\b`)
	reBigSerial     = regexp.MustCompile(`(?i)\bBIGSERIAL\b`)
	reSerial        = regexp.MustCompile(`(?i)\bSERIAL\b`)
	reTimestamptz   = regexp.MustCompile(`(?i)\bTIMESTAMPTZ\b`)
	// Plain TIMESTAMP (Prisma's default `DateTime` column mapping — most
	// of LivingImpactBudget's columns use this, not TIMESTAMPTZ) must be
	// mapped to TEXT exactly like TIMESTAMPTZ already is. Leaving a column
	// declared as SQLite type "TIMESTAMP" triggers modernc.org/sqlite's
	// own type-name-sniffing: it round-trips values through Go's time.Time
	// on scan/write instead of treating them as opaque text. That silently
	// breaks two things at once — (1) time.Time.String() renders in the
	// *server's local* timezone for values with no explicit zone (e.g. a
	// bound `Date` parameter) while a CURRENT_TIMESTAMP-defaulted value
	// renders in UTC, so two timestamps representing nearly the same
	// instant get stored as text in different timezones; (2) SQLite then
	// compares those TEXT values byte-for-byte in a WHERE clause, so a
	// range query mixing a CURRENT_TIMESTAMP-sourced column against
	// bound-parameter bounds can silently return zero rows even though the
	// values are seconds apart. Declaring the column as bare "TEXT"
	// instead sidesteps the driver's time.Time auto-conversion entirely,
	// so whatever text was written is exactly what's compared and returned.
	reTimestamp     = regexp.MustCompile(`(?i)\bTIMESTAMP\b`)
	reSmallint      = regexp.MustCompile(`(?i)\bSMALLINT\b`)
	reBoolean       = regexp.MustCompile(`(?i)\bBOOLEAN\b`)
	reVarchar       = regexp.MustCompile(`(?i)\bVARCHAR\b`)
	reDecimal       = regexp.MustCompile(`(?i)\bDECIMAL\b`)
	reNow           = regexp.MustCompile(`(?i)\bNOW\s*\(\s*\)`)
	reIlike         = regexp.MustCompile(`(?i)\bILIKE\b`)
	reTrue          = regexp.MustCompile(`(?i)\bTRUE\b`)
	reFalse         = regexp.MustCompile(`(?i)\bFALSE\b`)
	reCast          = regexp.MustCompile(`(?i)::\w+(?:\[\])?`)
	reDistinctOn    = regexp.MustCompile(`(?i)DISTINCT\s+ON\s*\([^)]+\)`)
	reDeleteAlias   = regexp.MustCompile(`(?i)^(DELETE\s+FROM\s+(\w+))\s+(\w+)\s+(WHERE.*)`)

	// DROP TABLE/VIEW/INDEX ... CASCADE|RESTRICT — SQLite has no CASCADE/
	// RESTRICT clause on DROP and rejects it outright with a syntax error.
	// Postgres migration tooling (including Prisma's `migrate reset` /
	// `db push --force-reset`) routinely appends CASCADE to DROP TABLE.
	// Stripping it is not a full semantic match (SQLite's DROP doesn't
	// cascade to dependent objects the way Postgres's does), but it makes
	// the statement parse for the common case of dropping one table/view/
	// index at a time, and SQLite's own foreign-key enforcement (enabled
	// via PRAGMA foreign_keys=ON in getDB) still blocks drops that would
	// orphan rows.
	reDropCascade   = regexp.MustCompile(`(?i)^(\s*DROP\s+(?:TABLE|VIEW|INDEX)\b.*?)\s+(?:CASCADE|RESTRICT)\s*$`)

	// ENUM types: SQLite has no CREATE TYPE. `CREATE TYPE <name> AS ENUM
	// (...)` is captured (name may be double-quoted, as Prisma always
	// emits) so the name can be registered and later substituted for TEXT
	// in CREATE TABLE column definitions; DROP TYPE / ALTER TYPE are no-ops
	// since there's no real type object to drop or extend.
	reCreateTypeEnum = regexp.MustCompile(`(?is)^\s*CREATE\s+TYPE\s+(?:"([^"]+)"|(\w+))\s+AS\s+ENUM\s*\(`)
	reVersionFunc    = regexp.MustCompile(`(?i)\bversion\s*\(\s*\)`)
	reDropType       = regexp.MustCompile(`(?is)^\s*DROP\s+TYPE\b`)
	reAlterType      = regexp.MustCompile(`(?is)^\s*ALTER\s+TYPE\b`)
	// SQLite has no schema/namespace concept — every Prisma Postgres
	// migration begins with `CREATE SCHEMA IF NOT EXISTS "public"`, which
	// otherwise fails outright as unrecognized syntax.
	reCreateSchema   = regexp.MustCompile(`(?is)^\s*CREATE\s+SCHEMA\b`)
	reAlterAddDropConstraint = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+\S+\s+(?:ADD|DROP)\s+CONSTRAINT\b`)
	reCountDistinctTuple = regexp.MustCompile(`(?i)COUNT\s*\(\s*DISTINCT\s*\(([^)]+)\)\s*\)`)
	reAny           = regexp.MustCompile(`(?i)=\s*ANY\s*\(\$(\d+)\)`)
	reAnyUnnest     = regexp.MustCompile(`(?i)=\s*ANY\s*\(\s*SELECT\s+unnest\s*\(\s*\$(\d+)\s*\)\s*\)`)
	reUnnest1       = regexp.MustCompile(`(?i)\bunnest\s*\(\s*\$(\d+)\s*\)\s*AS\s+(\w+)\s*\(\s*(\w+)\s*\)`)
	reUnnest2       = regexp.MustCompile(`(?i)\bunnest\s*\(\s*\$(\d+)\s*,\s*\$(\d+)\s*\)\s*AS\s+(\w+)\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)`)
	reUnnest3       = regexp.MustCompile(`(?i)\bunnest\s*\(\s*\$(\d+)\s*,\s*\$(\d+)\s*,\s*\$(\d+)\s*\)\s*AS\s+(\w+)\s*\(\s*(\w+)\s*,\s*(\w+)\s*,\s*(\w+)\s*\)`)

	// generate_series(start, end) [AS] alias → inline recursive CTE subquery.
	reGenerateSeries = regexp.MustCompile(`(?i)\bgenerate_series\s*\(([^)]+)\)\s*(?:AS\s+)?(\w+)`)
	reOffset         = regexp.MustCompile(`(?i)\bOFFSET\b`) // see translateSQL
)

// translateSQL converts PostgreSQL DDL/DML to SQLite-compatible SQL.
// reLeadingLineComment strips leading `-- comment` lines (and surrounding
// blank lines) before any other translation. Several of translateSQL's
// patterns are anchored to the start of the query (e.g. CREATE TYPE ... AS
// ENUM detection) and never matched when a query was prefixed with a
// comment — exactly the shape Prisma's own generated migration SQL uses
// ("-- CreateEnum\nCREATE TYPE ..."), silently falling through to SQLite
// as unrecognized syntax instead of being translated.
var reLeadingLineComment = regexp.MustCompile(`^(\s*--[^\n]*\n)+`)

// reSchemaQualifier strips a `"public".` or `public.` schema qualifier
// immediately preceding a table/identifier reference. Prisma's query
// engine always schema-qualifies every table it touches (`"public"."User"`)
// since real Postgres requires resolving which schema a bare table name
// lives in — but SQLite has no schema/namespace concept at all, so
// `"public"."User"` is parsed as "table \"User\" in an attached database
// named public", which doesn't exist. This broke every single query
// Prisma Client issued at runtime, not just DDL. Scoped to the literal
// schema name "public" (Prisma's default and what this proxy's CREATE
// SCHEMA no-op already assumes) so it doesn't strip an unrelated
// identifier that happens to be named "public".
var reSchemaQualifier = regexp.MustCompile(`(?i)"?\bpublic\b"?\.`)

func translateSQL(q string) string {
	q = reLeadingLineComment.ReplaceAllString(q, "")
	q = reSchemaQualifier.ReplaceAllString(q, "")

	ql := strings.ToLower(q)

	// Postgres allows a bare OFFSET with no LIMIT (Prisma emits exactly
	// this for findMany({ skip }) with no take); SQLite's grammar requires
	// OFFSET to follow a LIMIT, so this is a hard syntax error otherwise.
	// SQLite's documented idiom for "no limit" is LIMIT -1.
	if strings.Contains(ql, "offset") && !strings.Contains(ql, "limit") {
		q = reOffset.ReplaceAllString(q, "LIMIT -1 OFFSET")
		ql = strings.ToLower(q)
	}

	// PostgreSQL sequence functions have no SQLite equivalent; return a dummy value.
	if strings.Contains(ql, "setval(") ||
		strings.Contains(ql, "nextval(") ||
		strings.Contains(ql, "currval(") {
		return "SELECT 1"
	}

	// version() is SQLite's `sqlite_version()`'s Postgres equivalent, used
	// by most pg drivers/ORMs — including Prisma's connection-adapter
	// health check — as one of the very first queries on a new connection.
	// SQLite has no such function, so this errored with "no such function:
	// version" on literally every connection, which several clients
	// (Prisma among them) surface as a connection-level failure rather
	// than a query error — "Can't reach database server" even though the
	// TCP connection and handshake had already succeeded. Match the
	// ParameterStatus server_version already sent at startup (see
	// doStartup) so the two don't disagree.
	q = reVersionFunc.ReplaceAllString(q, "'PostgreSQL 15.0'")

	q = strings.TrimRight(q, " \t\r\n")
	q = reDropCascade.ReplaceAllString(q, "$1")

	// CREATE TYPE ... AS ENUM: register the name (for column-type
	// substitution below) and no-op the statement — SQLite has no
	// user-defined type system, so there's nothing to actually create.
	// Enum values are NOT validated/enforced: this proxy stores the column
	// as plain TEXT, matching its documented scope (fast local dev/CI, not
	// full constraint fidelity — see "ADD/DROP CONSTRAINT: silently
	// ignored" in the README). If invalid-value protection matters for a
	// given workload, enforce it in the application layer.
	if m := reCreateTypeEnum.FindStringSubmatch(q); m != nil {
		name := m[1]
		if name == "" {
			name = m[2]
		}
		registerEnumType(name)
		return ""
	}
	// DROP TYPE / ALTER TYPE (e.g. ADD VALUE): no real type object exists
	// to drop or extend, so both are no-ops.
	if reDropType.MatchString(q) || reAlterType.MatchString(q) {
		return ""
	}
	// CREATE SCHEMA: SQLite has no schema/namespace concept; every table
	// already lives in one flat namespace, so this is a no-op.
	if reCreateSchema.MatchString(q) {
		return ""
	}
	// ALTER TABLE ... ADD/DROP CONSTRAINT (including ADD CONSTRAINT ...
	// FOREIGN KEY, which is how Prisma always emits foreign keys — as a
	// separate statement after all CREATE TABLEs, specifically so it can
	// support circular references without knowing the full table graph
	// up front). SQLite fundamentally cannot add a constraint to an
	// already-created table via ALTER TABLE — a foreign key can only be
	// declared inline in CREATE TABLE, or added later via SQLite's full
	// rename-recreate-copy-drop procedure, which this proxy does not
	// attempt. This was already documented in the README ("ADD/DROP
	// CONSTRAINT: silently ignored") but had no actual implementation, so
	// every real Prisma migration's foreign-key statements hard-errored
	// instead. Silently ignoring them (matching the documented, accepted
	// limitation) means those relationships are NOT enforced at the
	// database level — the same tradeoff already accepted for CHECK/UNIQUE
	// constraints added this way.
	if reAlterAddDropConstraint.MatchString(q) {
		return ""
	}
	// Substitute any known enum type name used as a column type — e.g.
	// `"recurrenceType" "RecurrenceType" NOT NULL` (Prisma always quotes)
	// or the unquoted form — with TEXT. Prisma passes enum values as text
	// labels, and we store them as plain TEXT so reads round-trip correctly.
	// Scoped to CREATE TABLE/ALTER TABLE so an unrelated column or table
	// that happens to share a name with an enum elsewhere in the query isn't
	// touched.
	if strings.Contains(strings.ToUpper(q), "CREATE TABLE") || strings.Contains(strings.ToUpper(q), "ALTER TABLE") {
		for _, name := range enumTypeNames() {
			quoted := regexp.MustCompile(`"` + regexp.QuoteMeta(name) + `"`)
			q = quoted.ReplaceAllString(q, "TEXT")
			bare := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`)
			q = bare.ReplaceAllString(q, "TEXT")
		}
	}

	// Prisma emits CAST($N::text AS "schema"."EnumType") to coerce a text
	// parameter into an enum. Since SQLite has no enum types, and we store
	// enum values as plain TEXT, strip the cast entirely so the original
	// label string is inserted and returned.
	q = stripEnumCasts(q)

	q = reBigSerial.ReplaceAllString(q, "INTEGER")
	q = reSerial.ReplaceAllString(q, "INTEGER")
	q = reTimestamptz.ReplaceAllString(q, "TEXT")
	q = reTimestamp.ReplaceAllString(q, "TEXT")
	q = reSmallint.ReplaceAllString(q, "INTEGER")
	q = reBoolean.ReplaceAllString(q, "INTEGER")
	q = reVarchar.ReplaceAllString(q, "TEXT")
	q = reDecimal.ReplaceAllString(q, "REAL")
	q = reNow.ReplaceAllString(q, "CURRENT_TIMESTAMP")
	q = reIlike.ReplaceAllString(q, "LIKE")
	q = reTrue.ReplaceAllString(q, "1")
	q = reFalse.ReplaceAllString(q, "0")

	// generate_series MUST be translated BEFORE cast stripping ($N::bigint is a valid arg, strip inside).
	q = translateGenerateSeries(q)

	q = reCast.ReplaceAllString(q, "")
	q = reAddColIfNot.ReplaceAllString(q, "ADD COLUMN")
	// CREATE TABLE / CREATE INDEX IF NOT EXISTS need no translation —
	// SQLite supports that clause natively — so nothing further to do here.
	// DISTINCT ON (cols) is PostgreSQL-only; strip it. Data is deduplicated by the app.
	q = reDistinctOn.ReplaceAllString(q, "")
	// INTERVAL arithmetic: expr - INTERVAL 'N unit' → datetime(expr, '-N unit')
	q = reInterval.ReplaceAllStringFunc(q, func(m string) string {
		parts := reInterval.FindStringSubmatch(m)
		expr, n, unit := parts[1], parts[2], parts[3]
		if strings.EqualFold(expr, "CURRENT_TIMESTAMP") {
			expr = "'now'"
		}
		return fmt.Sprintf("datetime(%s, '-%s %s')", expr, n, unit)
	})
	// DATE_TRUNC('day', col) → date(col); other granularities use strftime
	q = reDateTrunc.ReplaceAllStringFunc(q, func(m string) string {
		parts := reDateTrunc.FindStringSubmatch(m)
		granularity, col := strings.ToLower(parts[1]), strings.TrimSpace(parts[2])
		switch granularity {
		case "day":
			return fmt.Sprintf("date(%s)", col)
		case "month":
			return fmt.Sprintf("strftime('%%Y-%%m-01', %s)", col)
		case "year":
			return fmt.Sprintf("strftime('%%Y-01-01', %s)", col)
		case "hour":
			return fmt.Sprintf("strftime('%%Y-%%m-%%dT%%H:00:00', %s)", col)
		default:
			return fmt.Sprintf("date(%s)", col)
		}
	})

	// COUNT(DISTINCT (col1, col2)) → COUNT(DISTINCT col1 || '|' || col2)
	q = reCountDistinctTuple.ReplaceAllStringFunc(q, func(m string) string {
		parts := reCountDistinctTuple.FindStringSubmatch(m)
		cols := strings.Split(parts[1], ",")
		for i := range cols {
			cols[i] = strings.TrimSpace(cols[i])
		}
		return "COUNT(DISTINCT " + strings.Join(cols, " || '|' || ") + ")"
	})

	// SQLite does not allow aliases on DELETE's target table.
	// Rewrite: DELETE FROM tbl alias WHERE → DELETE FROM tbl WHERE
	// then replace alias. with tbl. throughout.
	if m := reDeleteAlias.FindStringSubmatch(q); m != nil {
		table, alias := m[2], m[3]
		// Strip the alias token between table name and WHERE
		q = reDeleteAlias.ReplaceAllString(q, "$1 $4")
		// Replace remaining alias. references with table.
		q = regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(alias)+`\.`).
			ReplaceAllString(q, table+".")
	}

	return q
}

// translateGenerateSeries rewrites each generate_series(start, end) [AS] alias
// into an inline recursive CTE subquery that SQLite can execute.
//
// Runs BEFORE cast stripping so ::bigint in args (e.g. $2::bigint, $3::bigint)
// is still present; we strip casts inside this function.
//
// Text params ($N) bound by node-postgres become TEXT in SQLite by default.
// Recursive CTE termination depends on INTEGER comparison, so we wrap each $N
// with CAST(... AS INTEGER) to prevent infinite loops from type coercion.
func translateGenerateSeries(q string) string {
	if !strings.Contains(strings.ToLower(q), "generate_series") {
		return q
	}
	return reGenerateSeries.ReplaceAllStringFunc(q, func(m string) string {
		subs := reGenerateSeries.FindStringSubmatch(m)
		args, alias := subs[1], subs[2]

		commaIdx := strings.Index(args, ",")
		if commaIdx < 0 {
			return m
		}
		start := reCast.ReplaceAllString(strings.TrimSpace(args[:commaIdx]), "")
		end := reCast.ReplaceAllString(strings.TrimSpace(args[commaIdx+1:]), "")
		internal := "_" + alias

		// Wrap parameters in CAST(... AS INTEGER) so SQLite treats them as numbers.
		// Literal integers are unaffected by CAST, and text params become proper ints.
		return fmt.Sprintf(
			"(WITH RECURSIVE %s(n) AS (SELECT CAST(%s AS INTEGER) UNION ALL SELECT n+1 FROM %s WHERE n < CAST(%s AS INTEGER)) SELECT n AS %s FROM %s) AS %s",
			internal, start, internal, end, alias, internal, alias,
		)
	})
}

// expandStatement splits a single translated statement into multiple if needed.
// Currently handles ALTER TABLE with multiple ADD COLUMN clauses, which SQLite
// requires as separate statements.
func expandStatement(q string) []string {
	if m := reMultiAddCol.FindStringSubmatch(q); m != nil {
		prefix, rest := m[1], m[2]
		// Split on ", ADD COLUMN" boundaries.
		// Part 0 already contains "ADD COLUMN"; parts 1+ need it prepended.
		parts := reAddColSplit.Split(rest, -1)
		if len(parts) > 1 {
			out := make([]string, len(parts))
			for i, p := range parts {
				if i == 0 {
					out[i] = prefix + " " + strings.TrimSpace(p)
				} else {
					out[i] = prefix + " ADD COLUMN " + strings.TrimSpace(p)
				}
			}
			return out
		}
	}
	return []string{q}
}

// expandParams replaces $N placeholders with ? and builds args.
// Handles = ANY($N), = ANY(SELECT unnest($N)), and unnest(...) by expanding
// PostgreSQL array literals into inline SQLite-compatible SQL.
func expandParams(query string, params []interface{}) (string, []interface{}) {
	var sb strings.Builder
	var args []interface{}
	i := 0
	for i < len(query) {
		// Match `= ANY(SELECT unnest($N))` starting at current position.
		if loc := reAnyUnnest.FindStringIndex(query[i:]); loc != nil && loc[0] == 0 {
			m := reAnyUnnest.FindStringSubmatch(query[i:])
			n, _ := strconv.Atoi(m[1])
			if n >= 1 && n <= len(params) {
				elems := parsePGArray(fmt.Sprint(params[n-1]))
				ph := strings.Repeat("?,", len(elems))
				if len(ph) > 0 {
					ph = ph[:len(ph)-1]
				}
				sb.WriteString("IN (")
				sb.WriteString(ph)
				sb.WriteByte(')')
				for _, e := range elems {
					args = append(args, e)
				}
			}
			i += loc[1]
			continue
		}
		// Match `= ANY($N)` starting at current position.
		if loc := reAny.FindStringIndex(query[i:]); loc != nil && loc[0] == 0 {
			m := reAny.FindStringSubmatch(query[i:])
			n, _ := strconv.Atoi(m[1])
			if n >= 1 && n <= len(params) {
				elems := parsePGArray(fmt.Sprint(params[n-1]))
				ph := strings.Repeat("?,", len(elems))
				if len(ph) > 0 {
					ph = ph[:len(ph)-1]
				}
				sb.WriteString("IN (")
				sb.WriteString(ph)
				sb.WriteByte(')')
				for _, e := range elems {
					args = append(args, e)
				}
			}
			i += loc[1]
			continue
		}
		// Match unnest with 1 column.
		if loc := reUnnest1.FindStringIndex(query[i:]); loc != nil && loc[0] == 0 {
			m := reUnnest1.FindStringSubmatch(query[i:])
			n, _ := strconv.Atoi(m[1])
			alias, col := m[2], m[3]
			var elems []string
			if n >= 1 && n <= len(params) {
				elems = parsePGArray(fmt.Sprint(params[n-1]))
			}
			var parts []string
			for j, e := range elems {
				args = append(args, e)
				if j == 0 {
					parts = append(parts, fmt.Sprintf("SELECT ? AS %s", col))
				} else {
					parts = append(parts, "SELECT ?")
				}
			}
			if len(parts) == 0 {
				sb.WriteString(fmt.Sprintf("(SELECT NULL AS %s WHERE 0) AS %s", col, alias))
			} else {
				sb.WriteString(fmt.Sprintf("(%s) AS %s", strings.Join(parts, " UNION ALL "), alias))
			}
			i += loc[1]
			continue
		}
		// Match unnest with 2 columns.
		if loc := reUnnest2.FindStringIndex(query[i:]); loc != nil && loc[0] == 0 {
			m := reUnnest2.FindStringSubmatch(query[i:])
			n1, _ := strconv.Atoi(m[1])
			n2, _ := strconv.Atoi(m[2])
			alias, col1, col2 := m[3], m[4], m[5]
			var elems1, elems2 []string
			if n1 >= 1 && n1 <= len(params) {
				elems1 = parsePGArray(fmt.Sprint(params[n1-1]))
			}
			if n2 >= 1 && n2 <= len(params) {
				elems2 = parsePGArray(fmt.Sprint(params[n2-1]))
			}
			count := len(elems1)
			if len(elems2) < count {
				count = len(elems2)
			}
			var parts []string
			for j := 0; j < count; j++ {
				args = append(args, elems1[j], elems2[j])
				if j == 0 {
					parts = append(parts, fmt.Sprintf("SELECT ? AS %s, ? AS %s", col1, col2))
				} else {
					parts = append(parts, "SELECT ?, ?")
				}
			}
			if len(parts) == 0 {
				sb.WriteString(fmt.Sprintf("(SELECT NULL AS %s, NULL AS %s WHERE 0) AS %s", col1, col2, alias))
			} else {
				sb.WriteString(fmt.Sprintf("(%s) AS %s", strings.Join(parts, " UNION ALL "), alias))
			}
			i += loc[1]
			continue
		}
		// Match unnest with 3 columns.
		if loc := reUnnest3.FindStringIndex(query[i:]); loc != nil && loc[0] == 0 {
			m := reUnnest3.FindStringSubmatch(query[i:])
			n1, _ := strconv.Atoi(m[1])
			n2, _ := strconv.Atoi(m[2])
			n3, _ := strconv.Atoi(m[3])
			alias, col1, col2, col3 := m[4], m[5], m[6], m[7]
			var elems1, elems2, elems3 []string
			if n1 >= 1 && n1 <= len(params) {
				elems1 = parsePGArray(fmt.Sprint(params[n1-1]))
			}
			if n2 >= 1 && n2 <= len(params) {
				elems2 = parsePGArray(fmt.Sprint(params[n2-1]))
			}
			if n3 >= 1 && n3 <= len(params) {
				elems3 = parsePGArray(fmt.Sprint(params[n3-1]))
			}
			count := len(elems1)
			if len(elems2) < count {
				count = len(elems2)
			}
			if len(elems3) < count {
				count = len(elems3)
			}
			var parts []string
			for j := 0; j < count; j++ {
				args = append(args, elems1[j], elems2[j], elems3[j])
				if j == 0 {
					parts = append(parts, fmt.Sprintf("SELECT ? AS %s, ? AS %s, ? AS %s", col1, col2, col3))
				} else {
					parts = append(parts, "SELECT ?, ?, ?")
				}
			}
			if len(parts) == 0 {
				sb.WriteString(fmt.Sprintf("(SELECT NULL AS %s, NULL AS %s, NULL AS %s WHERE 0) AS %s", col1, col2, col3, alias))
			} else {
				sb.WriteString(fmt.Sprintf("(%s) AS %s", strings.Join(parts, " UNION ALL "), alias))
			}
			i += loc[1]
			continue
		}
		// Match $N.
		if query[i] == '$' {
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			if j > i+1 {
				n, _ := strconv.Atoi(query[i+1 : j])
				if n >= 1 && n <= len(params) {
					args = append(args, params[n-1])
				} else {
					args = append(args, nil)
				}
				sb.WriteByte('?')
				i = j
				continue
			}
		}
		sb.WriteByte(query[i])
		i++
	}
	return sb.String(), args
}

// parsePGArray parses a PostgreSQL array literal {a,b,c} or Go slice [a b c] into a string slice.
func parsePGArray(s string) []string {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return nil
	}
	// Go slice format: [a b c] or JS-style: [a, b, c]
	if s[0] == '[' && s[len(s)-1] == ']' {
		inner := s[1 : len(s)-1]
		if inner == "" {
			return nil
		}
		parts := strings.Split(inner, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			out = append(out, strings.Trim(p, `"`))
		}
		return out
	}
	// PostgreSQL array literal: {a,b,c}
	if len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}' {
		inner := s[1 : len(s)-1]
		if inner == "" {
			return nil
		}
		parts := strings.Split(inner, ",")
		out := make([]string, len(parts))
		for i, p := range parts {
			out[i] = strings.Trim(strings.TrimSpace(p), `"`)
		}
		return out
	}
	return []string{s}
}

// decodeParams converts raw frontend parameters into Go values suitable for
// SQLite. It handles both text and binary encodings, using the parameter OID
// (from Parse) and the per-message format codes (from Bind) to decide how to
// interpret each value.
func decodeParams(paramOIDs []uint32, formatCodes []int16, raw [][]byte) []interface{} {
	out := make([]interface{}, len(raw))
	for i, b := range raw {
		if b == nil {
			continue
		}
		// Determine format: 0 = text (default), 1 = binary. If no format codes are
		// supplied, the protocol says all parameters are text.
		binary := false
		if len(formatCodes) == 1 {
			binary = formatCodes[0] == 1
		} else if i < len(formatCodes) {
			binary = formatCodes[i] == 1
		}

		oid := uint32(25) // default to text
		if i < len(paramOIDs) {
			oid = paramOIDs[i]
		}

		if binary {
			out[i] = decodeBinaryParam(oid, b)
			continue
		}

		// Text format: normalize booleans and timestamps. Prisma sends
		// boolean parameters as text 'true'/'false' for INTEGER columns
		// (after BOOLEAN -> INTEGER translation), and node-postgres sends
		// JS Date values as RFC3339 strings.
		s := strings.ToLower(string(b))
		if s == "true" || s == "t" {
			out[i] = 1
		} else if s == "false" || s == "f" {
			out[i] = 0
		} else {
			out[i] = normalizeTimestampParam(string(b))
		}
	}
	return out
}

// decodeBinaryParam converts a PostgreSQL binary-encoded parameter to a Go
// value that SQLite can bind. Without this, []byte values are stored as BLOBs
// and read back as opaque bytes, breaking Prisma's Boolean/Enum/Integer
// columns (which are mapped to SQLite INTEGER/TEXT by translateSQL).
func decodeBinaryParam(oid uint32, b []byte) interface{} {
	switch oid {
	case 16: // bool
		if len(b) == 0 {
			return 0
		}
		if b[0] != 0 {
			return 1
		}
		return 0
	case 21: // int2
		if len(b) >= 2 {
			return int64(int16(binary.BigEndian.Uint16(b)))
		}
	case 23: // int4
		if len(b) >= 4 {
			return int64(int32(binary.BigEndian.Uint32(b)))
		}
	case 20: // int8
		if len(b) >= 8 {
			return int64(binary.BigEndian.Uint64(b))
		}
	case 700: // float4
		if len(b) >= 4 {
			bits := binary.BigEndian.Uint32(b)
			return float64(math.Float32frombits(bits))
		}
	case 701: // float8
		if len(b) >= 8 {
			bits := binary.BigEndian.Uint64(b)
			return math.Float64frombits(bits)
		}
	}
	// Unknown OID or unhandled format: fall back to text. This covers enum
	// values when the OID is not one of the well-known numeric types; Prisma
	// may send enums as text or as an unregistered OID, and preserving the
	// original bytes as a string is the safest default.
	return normalizeTimestampParam(string(b))
}

// timestampParamLayouts are the text-protocol formats node-postgres (and
// other pg clients) actually send for a bound Date/timestamp parameter.
// node-postgres in particular serializes JS Date values using the *local*
// system timezone with an explicit offset (e.g.
// "2026-07-08T18:33:02.370-05:00"), not UTC/"Z" — it does not use
// Date.prototype.toISOString().
var timestampParamLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05-07:00",
}

// normalizeTimestampParam rewrites a timestamp-shaped bound parameter into
// the exact text format SQLite's own CURRENT_TIMESTAMP produces: UTC,
// "YYYY-MM-DD HH:MM:SS", no offset suffix, no sub-second digits.
//
// Timestamps in this proxy are stored as opaque TEXT (see the TIMESTAMP ->
// TEXT translation in translateSQL) and compared with plain string
// operators in SQL — there is no temporal parsing on the SQLite side at
// all. That only produces correct orderings if every timestamp value ever
// written is in the same textual format. Without this, a column populated
// via `DEFAULT CURRENT_TIMESTAMP` (UTC, no offset) and the same column
// populated via a bound `Date` parameter (local timezone, explicit offset,
// milliseconds) hold values that represent nearly the same instant but
// compare incorrectly as strings — a range query spanning both sources can
// silently return the wrong rows (including zero rows) with no error.
//
// Values that don't parse as one of the known timestamp layouts are
// returned unchanged — this only touches parameters that are actually
// timestamp-shaped, so it does not affect ordinary text/number parameters.
func normalizeTimestampParam(s string) string {
	for _, layout := range timestampParamLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Format("2006-01-02 15:04:05")
		}
	}
	return s
}

// splitStatements splits a raw SQL string on semicolons while respecting
// string literals and dollar-quoted blocks.
func splitStatements(raw string) []string {
	var out []string
	var current strings.Builder
	inString := false
	stringQuote := byte(0)
	inDollar := false
	var dollarTag string

	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if inString {
			current.WriteByte(c)
			if c == stringQuote {
				if i+1 < len(raw) && raw[i+1] == stringQuote {
					current.WriteByte(raw[i+1])
					i++
				} else {
					inString = false
				}
			}
			continue
		}
		if inDollar {
			current.WriteByte(c)
			if c == '$' {
				tagEnd := i + 1 + len(dollarTag)
				if tagEnd <= len(raw) && raw[i+1:tagEnd] == dollarTag && (tagEnd == len(raw) || raw[tagEnd] == '$') {
					inDollar = false
				}
			}
			continue
		}
		if c == '\'' || c == '"' {
			inString = true
			stringQuote = c
			current.WriteByte(c)
			continue
		}
		if c == '$' {
			j := i + 1
			for j < len(raw) && (raw[j] == '_' || (raw[j] >= 'a' && raw[j] <= 'z') || (raw[j] >= 'A' && raw[j] <= 'Z') || (raw[j] >= '0' && raw[j] <= '9')) {
				j++
			}
			if j > i+1 {
				dollarTag = raw[i+1 : j]
				inDollar = true
				current.WriteByte(c)
				continue
			}
		}
		if c == ';' {
			out = append(out, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}
	if current.Len() > 0 {
		out = append(out, current.String())
	}
	return out
}
