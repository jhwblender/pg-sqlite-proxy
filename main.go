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
		path = singleDBPath + "?_journal_mode=WAL&_foreign_keys=on&_busy_timeout=5000"
	} else {
		if err := os.MkdirAll("dbs", 0755); err != nil {
			return nil, fmt.Errorf("create dir: %w", err)
		}
		path = fmt.Sprintf("dbs/%s.db?_journal_mode=WAL&_foreign_keys=on&_busy_timeout=5000", name)
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

		case *pgproto3.Close:
			if m.ObjectType == 'S' {
				delete(stmts, m.Name)
			} else {
				delete(portals, m.Name)
			}
			be.Send(&pgproto3.CloseComplete{})
			be.Flush()

		case *pgproto3.Sync:
			rfq(be, txStatus(inTx))

		case *pgproto3.Flush:
			be.Flush()
		}
	}
}

func txStatus(inTx bool) byte {
	if inTx {
		return 'T'
	}
	return 'I'
}

func doStartup(be *pgproto3.Backend, conn net.Conn) (string, error) {
	msg, err := be.ReceiveStartupMessage()
	if err != nil {
		return "", err
	}
	if _, ok := msg.(*pgproto3.SSLRequest); ok {
		if _, err := conn.Write([]byte("N")); err != nil {
			return "", err
		}
		msg, err = be.ReceiveStartupMessage()
		if err != nil {
			return "", err
		}
	}
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
		log.Printf("query error: %v | sql: %s", err, q)
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
				Name:         []byte(c),
				DataTypeOID:  oids[i],
				DataTypeSize: -1,
				TypeModifier: -1,
			}
		}
		be.Send(&pgproto3.RowDescription{Fields: fields})
	}

	for _, row := range buffered {
		encoded := make([][]byte, len(cols))
		for i, v := range row {
			encoded[i] = toText(v)
		}
		be.Send(&pgproto3.DataRow{Values: encoded})
	}
	be.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(buffered)))})
	return false
}

func runDML(be *pgproto3.Backend, sqlConn *sql.Conn, q string, args []interface{}) (failed bool) {
	res, err := sqlConn.ExecContext(context.Background(), q, args...)
	if err != nil {
		log.Printf("exec error: %v | sql: %s", err, q)
		sendErr(be, err.Error())
		return true
	}
	var n int64
	u := strings.ToUpper(strings.TrimSpace(q))
	if strings.HasPrefix(u, "INSERT") || strings.HasPrefix(u, "UPDATE") || strings.HasPrefix(u, "DELETE") {
		n, _ = res.RowsAffected()
	}
	be.Send(&pgproto3.CommandComplete{CommandTag: []byte(cmdTag(q, n))})
	return false
}

// ── SQL translation ────────────────────────────────────────────────────────

// Pre-compiled regexps for translateSQL — compiled once at startup.
var (
	reAny = regexp.MustCompile(`(?i)=\s*ANY\s*\(\s*\$(\d+)\s*\)`)
	reNum = regexp.MustCompile(`\$(\d+)`)

	reDeleteAlias = regexp.MustCompile(`(?i)^(DELETE\s+FROM\s+([\w.]+))\s+(\w+)\s+(WHERE\b)`)

	reBigSerial    = regexp.MustCompile(`(?i)\bBIGSERIAL\b`)
	reSerial       = regexp.MustCompile(`(?i)\bSERIAL\b`)
	reTimestamptz  = regexp.MustCompile(`(?i)\bTIMESTAMPTZ\b`)
	reSmallint     = regexp.MustCompile(`(?i)\bSMALLINT\b`)
	reBoolean      = regexp.MustCompile(`(?i)\bBOOLEAN\b`)
	reVarchar      = regexp.MustCompile(`(?i)\bVARCHAR\s*\(\d+\)`)
	reDecimal      = regexp.MustCompile(`(?i)\bDECIMAL\s*\(\d+\s*,\s*\d+\)`)
	reNow          = regexp.MustCompile(`(?i)\bNOW\(\)`)
	reIlike        = regexp.MustCompile(`(?i)\bILIKE\b`)
	reTrue         = regexp.MustCompile(`(?i)\bTRUE\b`)
	reFalse        = regexp.MustCompile(`(?i)\bFALSE\b`)
	reCast         = regexp.MustCompile(`::\w+(\[\])?`)
	reAddColIfNot  = regexp.MustCompile(`(?i)\bADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\b`)
	reDistinctOn   = regexp.MustCompile(`(?i)DISTINCT\s+ON\s*\([^)]+\)`)
	reInterval     = regexp.MustCompile(`(?i)([\w.'"]+)\s*-\s*INTERVAL\s+'(\d+)\s+(days?|hours?|minutes?|seconds?)'`)
	reDateTrunc    = regexp.MustCompile(`(?i)DATE_TRUNC\s*\(\s*'(\w+)'\s*,\s*([^)]+)\)`)

	reMultiAddCol = regexp.MustCompile(`(?is)^(ALTER\s+TABLE\s+\w+)\s+(ADD\s+COLUMN\b.+)$`)
	reAddColSplit = regexp.MustCompile(`(?i),\s*ADD\s+COLUMN\b`)

	reSelect    = regexp.MustCompile(`(?i)^\s*(SELECT|WITH|EXPLAIN)\b`)
	reReturning = regexp.MustCompile(`(?i)\bRETURNING\b`)
	reColFunc   = regexp.MustCompile(`^(\w+)\(.*\)$`)

	// COUNT(DISTINCT (col1, col2)) is a PostgreSQL row-value expression; SQLite
	// doesn't support it. Rewrite to COUNT(DISTINCT col1 || '|' || col2).
	reCountDistinctTuple = regexp.MustCompile(`(?i)COUNT\s*\(\s*DISTINCT\s*\(\s*([^)]+)\s*\)\s*\)`)

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

	// generate_series(start, end) [AS] alias → inline recursive CTE subquery.
	q = translateGenerateSeries(q)

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
// into an inline recursive CTE subquery that SQLite can execute:
//
//	(WITH RECURSIVE _alias(n) AS
//	   (SELECT start UNION ALL SELECT n+1 FROM _alias WHERE n < end)
//	 SELECT n AS alias FROM _alias) AS alias
//
// The internal CTE uses a prefixed name to avoid shadowing the outer alias.
// This handles both the CROSS JOIN grid pattern and the UNION ALL border pattern
// without requiring restructuring of the surrounding query.
func translateGenerateSeries(q string) string {
	if !strings.Contains(strings.ToLower(q), "generate_series") {
		return q
	}
	return reGenerateSeries.ReplaceAllStringFunc(q, func(m string) string {
		subs := reGenerateSeries.FindStringSubmatch(m)
		args, alias := subs[1], subs[2]

		commaIdx := strings.Index(args, ",")
		if commaIdx < 0 {
			return m // unexpected form, leave unchanged
		}
		start := strings.TrimSpace(args[:commaIdx])
		end := strings.TrimSpace(args[commaIdx+1:])
		internal := "_" + alias

		// generate_series is inclusive on both ends; the CTE achieves this because
		// the recursion stops when n reaches end (WHERE n < end stops *after* end
		// is already in the result set from the previous step).
		return fmt.Sprintf(
			"(WITH RECURSIVE %s(n) AS (SELECT %s UNION ALL SELECT n+1 FROM %s WHERE n < %s) SELECT n AS %s FROM %s) AS %s",
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
// Handles = ANY($N) by expanding the PostgreSQL array literal into IN (?,?,...).
func expandParams(query string, params []interface{}) (string, []interface{}) {
	var sb strings.Builder
	var args []interface{}
	i := 0
	for i < len(query) {
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

// parsePGArray parses a PostgreSQL array literal {a,b,c} into a string slice.
func parsePGArray(s string) []string {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		if s != "" {
			return []string{s}
		}
		return nil
	}
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

// splitStatements splits SQL on semicolons, skipping those inside string literals,
// -- line comments, and /* block comments */.
func splitStatements(sql string) []string {
	var stmts []string
	var sb strings.Builder
	inStr, inLine, inBlock := false, false, false
	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case !inStr && !inBlock && !inLine && c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			inLine = true
			i += 2
		case inLine && c == '\n':
			inLine = false
			sb.WriteByte(c)
			i++
		case inLine:
			i++
		case !inStr && !inLine && c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			inBlock = true
			i += 2
		case inBlock && c == '*' && i+1 < len(sql) && sql[i+1] == '/':
			inBlock = false
			i += 2
		case inBlock:
			i++
		case c == '\'' && !inStr:
			inStr = true
			sb.WriteByte(c)
			i++
		case c == '\'' && inStr:
			inStr = false
			sb.WriteByte(c)
			i++
		case c == ';' && !inStr:
			if s := strings.TrimSpace(sb.String()); s != "" {
				stmts = append(stmts, s)
			}
			sb.Reset()
			i++
		default:
			sb.WriteByte(c)
			i++
		}
	}
	if s := strings.TrimSpace(sb.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// isNoOp returns true for PG-specific statements SQLite cannot run and should ignore.
func isNoOp(q string) bool {
	u := strings.ToUpper(strings.TrimSpace(q))
	return strings.Contains(u, "PG_ADVISORY") ||
		strings.Contains(u, "DROP CONSTRAINT") ||
		strings.Contains(u, "ADD CONSTRAINT")
}

func isSelect(q string) bool {
	return reSelect.MatchString(q) || reReturning.MatchString(q)
}

// sqliteOID maps a SQLite declared type name to a PostgreSQL OID so the pg
// client parses numeric columns as JS numbers instead of strings.
func sqliteOID(typeName string) uint32 {
	t := strings.ToUpper(strings.TrimSpace(typeName))
	switch {
	case strings.Contains(t, "INT"):
		return 23 // int4 — node-postgres returns JS number (not bigint string)
	case strings.Contains(t, "REAL"),
		strings.Contains(t, "FLOAT"),
		strings.Contains(t, "DOUBLE"),
		strings.Contains(t, "NUMERIC"),
		strings.Contains(t, "DECIMAL"):
		return 701 // float8
	default:
		return 25 // text
	}
}

// normalizeCols cleans up SQLite column names to match PostgreSQL conventions.
// e.g. SQLite returns "count(*)" where PostgreSQL returns "count".
func normalizeCols(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		if m := reColFunc.FindStringSubmatch(c); m != nil {
			out[i] = m[1] // e.g. "count(*)" → "count"
		} else {
			out[i] = c
		}
	}
	return out
}

func toText(v interface{}) []byte {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case float64:
		return []byte(strconv.FormatFloat(val, 'f', -1, 64))
	case float32:
		return []byte(strconv.FormatFloat(float64(val), 'f', -1, 32))
	default:
		return []byte(fmt.Sprint(val))
	}
}

func cmdTag(q string, n int64) string {
	u := strings.ToUpper(strings.TrimSpace(q))
	switch {
	case strings.HasPrefix(u, "INSERT"):
		return fmt.Sprintf("INSERT 0 %d", n)
	case strings.HasPrefix(u, "UPDATE"):
		return fmt.Sprintf("UPDATE %d", n)
	case strings.HasPrefix(u, "DELETE"):
		return fmt.Sprintf("DELETE %d", n)
	case strings.HasPrefix(u, "BEGIN"):
		return "BEGIN"
	case strings.HasPrefix(u, "COMMIT"):
		return "COMMIT"
	case strings.HasPrefix(u, "ROLLBACK"):
		return "ROLLBACK"
	case strings.HasPrefix(u, "CREATE"):
		return "CREATE TABLE"
	case strings.HasPrefix(u, "ALTER"):
		return "ALTER TABLE"
	case strings.HasPrefix(u, "DROP"):
		return "DROP TABLE"
	default:
		return "OK"
	}
}

func sendErr(be *pgproto3.Backend, msg string) {
	log.Printf("pg-proxy error: %s", msg)
	be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: msg})
}

func rfq(be *pgproto3.Backend, status byte) {
	be.Send(&pgproto3.ReadyForQuery{TxStatus: status})
	be.Flush()
}
