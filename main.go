package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	dbs          = make(map[string]*sql.DB)
	dbsMu        sync.Mutex
	singleDBPath string // non-empty: all connections use this one file
)

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
}

type portal struct {
	stmt   *prepStmt
	params []interface{}
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
		switch m := msg.(type) {
		case *pgproto3.Terminate:
			return

		case *pgproto3.Query:
			inTx = simpleQuery(be, sqlConn, m.String, inTx)

		case *pgproto3.Parse:
			stmts[m.Name] = &prepStmt{
				query:     translateSQL(m.Query),
				paramOIDs: m.ParameterOIDs,
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
				stmt:   stmt,
				params: decodeParams(m.Parameters),
			}
			be.Send(&pgproto3.BindComplete{})

		case *pgproto3.Describe:
			if m.ObjectType == 'S' {
				stmt, ok := stmts[m.Name]
				if !ok {
					sendErr(be, "unknown statement: "+m.Name)
					be.Flush()
					continue
				}
				be.Send(&pgproto3.ParameterDescription{ParameterOIDs: stmt.paramOIDs})
			}
			// RowDescription is sent in Execute for SELECT; NoData for everything else.
			be.Send(&pgproto3.NoData{})
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
		if strings.HasPrefix(u, "BEGIN") {
			inTx = true
		} else if strings.HasPrefix(u, "COMMIT") || strings.HasPrefix(u, "ROLLBACK") {
			inTx = false
		}

		var failed bool
		for _, t := range expandStatement(translateSQL(s)) {
			if isNoOp(t) {
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
				continue
			}
			if isSelect(t) {
				failed = runSelect(be, sqlConn, t, nil, true)
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

	u := strings.ToUpper(strings.TrimSpace(q))
	if strings.HasPrefix(u, "BEGIN") {
		inTx = true
	} else if strings.HasPrefix(u, "COMMIT") || strings.HasPrefix(u, "ROLLBACK") {
		inTx = false
	}

	if isNoOp(q) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
		return inTx
	}
	if isSelect(q) {
		runSelect(be, sqlConn, q, args, true)
	} else {
		runDML(be, sqlConn, q, args)
	}
	return inTx
}

func runSelect(be *pgproto3.Backend, sqlConn *sql.Conn, q string, args []interface{}, sendHeader bool) (failed bool) {
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
		if oid == 25 {
			for _, row := range buffered {
				if row[i] == nil {
					continue
				}
				switch row[i].(type) {
				case int64:
					oid = 23 // int4 — node-postgres returns JS number (not bigint string)
				case float64:
					oid = 701 // float8
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

	be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT ")})
	return false
}

func runDML(be *pgproto3.Backend, sqlConn *sql.Conn, q string, args []interface{}) bool {
	res, err := sqlConn.ExecContext(context.Background(), q, args...)
	if err != nil {
		log.Printf("exec error: %v | sql: %s", err, q)
		sendErr(be, err.Error())
		return true
	}

	// Try to get rows affected.
	tag := "OK"
	if ra, err := res.RowsAffected(); err == nil {
		tag = fmt.Sprintf("%d", ra)
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
func sqliteOID(declared string) uint32 {
	u := strings.ToUpper(declared)
	switch u {
	case "INTEGER":
		return 23 // int4
	case "REAL", "NUMERIC", "DOUBLE", "FLOAT":
		return 701 // float8
	case "BLOB":
		return 17 // bytea
	case "TEXT", "VARCHAR", "CHAR", "CLOB":
		return 25 // text
	case "NULL":
		return 25 // text (fallback)
	default:
		return 25 // text
	}
}

func isNoOp(q string) bool {
	u := strings.ToUpper(strings.TrimSpace(q))
	return u == "" ||
		strings.HasPrefix(u, "SET ") ||
		strings.HasPrefix(u, "SHOW ") ||
		strings.HasPrefix(u, "COMMIT") ||
		strings.HasPrefix(u, "ROLLBACK") ||
		strings.HasPrefix(u, "BEGIN")
}

func isSelect(q string) bool {
	u := strings.ToUpper(strings.TrimSpace(q))
	return strings.HasPrefix(u, "SELECT")
}

// normalizeCols rewrites fully-qualified column references to bare names.
func normalizeCols(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		idx := strings.LastIndex(c, ".")
		if idx >= 0 {
			out[i] = c[idx+1:]
		} else {
			out[i] = c
		}
	}
	return out
}

var (
	// reInterval: e.g. CURRENT_TIMESTAMP - INTERVAL '24 hour' or col - INTERVAL '1 day'
	reInterval = regexp.MustCompile(`(?i)([\w.]+)\s+-\s+INTERVAL\s+'(\d+)\s+(\w+)'`)

	reDateTrunc = regexp.MustCompile(`(?i)DATE_TRUNC\s*\(\s*'([^']+)'\s*,\s*([^)]+)\s*\)`)

	reMultiAddCol   = regexp.MustCompile(`(?i)^(ALTER\s+TABLE\s+\w+\s+ADD\s+COLUMN\s+.+)`)
	reAddColSplit   = regexp.MustCompile(`(?i),\s*ADD\s+COLUMN\s+`)
	reAddColIfNot   = regexp.MustCompile(`(?i)\bIF\s+NOT\s+EXISTS\b`)
	reBigSerial     = regexp.MustCompile(`(?i)\bBIGSERIAL\b`)
	reSerial        = regexp.MustCompile(`(?i)\bSERIAL\b`)
	reTimestamptz   = regexp.MustCompile(`(?i)\bTIMESTAMPTZ\b`)
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
	reCountDistinctTuple = regexp.MustCompile(`(?i)COUNT\s*\(\s*DISTINCT\s*\(([^)]+)\)\s*\)`)
	reAny           = regexp.MustCompile(`(?i)=\s*ANY\s*\(\$(\d+)\)`)
	reAnyUnnest     = regexp.MustCompile(`(?i)=\s*ANY\s*\(\s*SELECT\s+unnest\s*\(\s*\$(\d+)\s*\)\s*\)`)
	reUnnest1       = regexp.MustCompile(`(?i)\bunnest\s*\(\s*\$(\d+)\s*\)\s*AS\s+(\w+)\s*\(\s*(\w+)\s*\)`)
	reUnnest2       = regexp.MustCompile(`(?i)\bunnest\s*\(\s*\$(\d+)\s*,\s*\$(\d+)\s*\)\s*AS\s+(\w+)\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)`)
	reUnnest3       = regexp.MustCompile(`(?i)\bunnest\s*\(\s*\$(\d+)\s*,\s*\$(\d+)\s*,\s*\$(\d+)\s*\)\s*AS\s+(\w+)\s*\(\s*(\w+)\s*,\s*(\w+)\s*,\s*(\w+)\s*\)`)

	// generate_series(start, end) [AS] alias → inline recursive CTE subquery.
	reGenerateSeries = regexp.MustCompile(`(?i)\bgenerate_series\s*\(([^)]+)\)\s*(?:AS\s+)?(\w+)`)
)

// translateSQL converts PostgreSQL DDL/DML to SQLite-compatible SQL.
func translateSQL(q string) string {
	// PostgreSQL sequence functions have no SQLite equivalent; return a dummy value.
	ql := strings.ToLower(q)
	if strings.Contains(ql, "setval(") ||
		strings.Contains(ql, "nextval(") ||
		strings.Contains(ql, "currval(") {
		return "SELECT 1"
	}

	q = reBigSerial.ReplaceAllString(q, "INTEGER")
	q = reSerial.ReplaceAllString(q, "INTEGER")
	q = reTimestamptz.ReplaceAllString(q, "TEXT")
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

// decodeParams converts raw parameter bytes to Go string values (text protocol).
func decodeParams(raw [][]byte) []interface{} {
	out := make([]interface{}, len(raw))
	for i, b := range raw {
		if b != nil {
			out[i] = string(b)
		}
	}
	return out
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
