package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"

	_ "modernc.org/sqlite"
)

const DB_PATH = "C:\\Users\\as74t\\Documents\\infinite-pixels-database\\infinite-pixels.db"
const PROXY_PORT = 5433

var db *sql.DB

func main() {
	var err error

	db, err = sql.Open("sqlite", DB_PATH)
	if err != nil {
		log.Fatalf("Failed to open SQLite: %v", err)
	}

	// Create database file by executing a simple query
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping SQLite: %v", err)
	}

	// Explicitly create the database file by executing a simple query
	_, err = db.Exec("SELECT 1")
	if err != nil {
		log.Fatalf("Failed to execute test query: %v", err)
	}

	fmt.Println("SQLite database connected at:", DB_PATH)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", PROXY_PORT))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", PROXY_PORT, err)
	}
	defer listener.Close()

	fmt.Printf("PostgreSQL proxy listening on port %d\n", PROXY_PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Client connected")

	buf := make([]byte, 4)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("Failed to read startup: %v\n", err)
		return
	}

	msgLen := int(binary.BigEndian.Uint32(buf))

	msg := make([]byte, msgLen-4)
	_, err = conn.Read(msg)
	if err != nil {
		fmt.Printf("Failed to read startup message: %v\n", err)
		return
	}

	fmt.Printf("Startup message length: %d\n", msgLen)
	fmt.Printf("Startup message bytes: %v\n", msg)
	fmt.Printf("Startup message (decoded): %q\n", string(msg))

	msg = bytes.TrimRight(msg, "\x00")
	fmt.Printf("Startup message (trimmed): %q\n", string(msg))

	// Send Authentication OK (type 0 = ok)
	authMsg := make([]byte, 9)
	authMsg[0] = 'R'
	binary.BigEndian.PutUint32(authMsg[1:5], 9)
	binary.BigEndian.PutUint32(authMsg[5:9], 0)
	conn.Write(authMsg)

	// Send ParameterStatus (server_version)
	serverVersion := "9.6.0"
	versionBuf := new(bytes.Buffer)
	versionBuf.WriteByte('S')
	versionBuf.Write([]byte{0, 0, 0, 0})
	versionBuf.WriteByte(0)
	versionBuf.Write([]byte("server_version\x00"))
	versionBuf.Write([]byte(serverVersion + "\x00"))
	length := versionBuf.Len() - 5
	versionData := versionBuf.Bytes()
	binary.BigEndian.PutUint32(versionData[1:5], uint32(length))
	conn.Write(versionData)

	// Send BackendKeyData
	keyData := make([]byte, 13)
	keyData[0] = 'K'
	binary.BigEndian.PutUint32(keyData[1:5], 13)
	binary.BigEndian.PutUint32(keyData[5:9], 12345)
	binary.BigEndian.PutUint32(keyData[9:13], 67890)
	fmt.Printf("Sending BackendKeyData: %v\n", keyData)
	conn.Write(keyData)

	// Send ReadyForQuery (transaction idle state)
	ready := make([]byte, 6)
	ready[0] = 'Z'
	binary.BigEndian.PutUint32(ready[1:5], 5)
	ready[5] = 'I'
	fmt.Printf("Sending ReadyForQuery: %v\n", ready)
	conn.Write(ready)
	fmt.Printf("Sent ReadyForQuery, waiting for client messages\n")

	fmt.Println("Startup complete, waiting for queries")

	for {
		msgType := make([]byte, 1)
		n, err := conn.Read(msgType)
		if err != nil {
			fmt.Printf("Error reading message type: %v, n=%d\n", err, n)
			fmt.Println("Client disconnected")
			return
		}
		fmt.Printf("Received message type: %c (0x%02x)\n", msgType[0], msgType[0])

		if msgType[0] == 'X' {
			fmt.Println("Client exited")
			return
		}

		lenBuf := make([]byte, 4)
		_, err = conn.Read(lenBuf)
		if err != nil {
			fmt.Printf("Error reading length: %v\n", err)
			return
		}
		msgLen := int(binary.BigEndian.Uint32(lenBuf))
		fmt.Printf("Message length: %d\n", msgLen)

		msg := make([]byte, msgLen-4)
		_, err = conn.Read(msg)
		if err != nil {
			fmt.Printf("Error reading message: %v\n", err)
			return
		}

		switch msgType[0] {
		case 'Q':
			query := strings.TrimRight(string(msg), "\x00")
			fmt.Printf("Query: %s\n", query)
			handleQuery(conn, query)
		case 'P':
			handleParse(conn, msg)
		case 'B':
			handleBind(conn, msg)
		case 'E':
			handleExecute(conn, msg)
		case 'C':
			handleClose(conn, msg)
		case 'D':
			handleDescribe(conn, msg)
		default:
			fmt.Printf("Unknown message type: %c\n", msgType[0])
		}
	}
}

func handleQuery(conn net.Conn, query string) {
	translated := translateQuery(query)
	fmt.Printf("Translated: %s\n", translated)

	rows, err := db.Query(translated)
	if err != nil {
		sendErrorResponse(conn, err.Error())
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		sendErrorResponse(conn, err.Error())
		return
	}

	sendRowDescription(conn, columns)

	for rows.Next() {
		vals := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		sendDataRow(conn, vals)
	}

	sendCommandComplete(conn, "SELECT 1")
	sendReadyForQuery(conn)
}

func handleParse(conn net.Conn, msg []byte) {
	pos := 0
	queryEnd := bytes.IndexByte(msg[pos:], 0)
	query := string(msg[pos : pos+queryEnd])

	fmt.Printf("Parse: %s\n", query)

	parseComplete := make([]byte, 5)
	parseComplete[0] = '1'
	binary.BigEndian.PutUint32(parseComplete[1:5], 5)
	conn.Write(parseComplete)
}

func handleBind(conn net.Conn, msg []byte) {
	bindComplete := make([]byte, 5)
	bindComplete[0] = '2'
	binary.BigEndian.PutUint32(bindComplete[1:5], 5)
	conn.Write(bindComplete)
}

func handleExecute(conn net.Conn, msg []byte) {
	sendCommandComplete(conn, "EXECUTE")
	sendReadyForQuery(conn)
}

func handleClose(conn net.Conn, msg []byte) {
	closeComplete := make([]byte, 5)
	closeComplete[0] = '3'
	binary.BigEndian.PutUint32(closeComplete[1:5], 5)
	conn.Write(closeComplete)
}

func handleDescribe(conn net.Conn, msg []byte) {
	columns := []string{"column1"}
	sendRowDescription(conn, columns)
}

func sendRowDescription(conn net.Conn, columns []string) {
	buf := new(bytes.Buffer)
	buf.WriteByte('T')
	buf.Write([]byte{0, 0, 0, 0})
	buf.WriteByte(0)
	buf.WriteByte(byte(len(columns)))

	for _, col := range columns {
		buf.Write([]byte(col + "\x00"))
		buf.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	}

	length := buf.Len() - 1
	buf2 := buf.Bytes()
	binary.BigEndian.PutUint32(buf2[1:5], uint32(length))

	conn.Write(buf2)
}

func sendDataRow(conn net.Conn, values []interface{}) {
	buf := new(bytes.Buffer)
	buf.WriteByte('D')
	buf.Write([]byte{0, 0, 0, 0})
	buf.WriteByte(0)
	buf.WriteByte(byte(len(values)))

	for _, val := range values {
		if val == nil {
			buf.Write([]byte{255, 255, 255, 255})
		} else {
			valStr := fmt.Sprintf("%v", val)
			valBuf := []byte(valStr)
			buf.Write([]byte{byte(len(valStr) >> 24), byte(len(valStr) >> 16), byte(len(valStr) >> 8), byte(len(valStr))})
			buf.Write(valBuf)
		}
	}

	length := buf.Len() - 1
	buf2 := buf.Bytes()
	binary.BigEndian.PutUint32(buf2[1:5], uint32(length))

	conn.Write(buf2)
}

func sendErrorResponse(conn net.Conn, msg string) {
	buf := new(bytes.Buffer)
	buf.WriteByte('E')
	buf.Write([]byte{0, 0, 0, 0})
	buf.WriteByte('C')
	buf.Write([]byte(msg + "\x00"))

	length := buf.Len() - 1
	buf2 := buf.Bytes()
	binary.BigEndian.PutUint32(buf2[1:5], uint32(length))

	conn.Write(buf2)
	sendReadyForQuery(conn)
}

func sendCommandComplete(conn net.Conn, cmd string) {
	buf := []byte(cmd + "\x00")
	length := len(buf) + 4

	result := make([]byte, length)
	result[0] = 'C'
	binary.BigEndian.PutUint32(result[1:5], uint32(length))
	copy(result[5:], buf)

	conn.Write(result)
}

func sendReadyForQuery(conn net.Conn) {
	ready := make([]byte, 5)
	ready[0] = 'Z'
	binary.BigEndian.PutUint32(ready[1:5], 5)
	ready[4] = 'I'
	conn.Write(ready)
}

func translateQuery(sql string) string {
	translated := sql

	re := regexp.MustCompile(`\$\d+`)
	translated = re.ReplaceAllString(translated, "?")

	translated = strings.ReplaceAll(translated, "NOW()", "strftime('%s', 'now')")
	translated = strings.ReplaceAll(translated, "TRUE", "1")
	translated = strings.ReplaceAll(translated, "FALSE", "0")
	translated = strings.ReplaceAll(translated, "SERIAL", "INTEGER")
	translated = strings.ReplaceAll(translated, "BIGSERIAL", "INTEGER")
	translated = strings.ReplaceAll(translated, "BOOLEAN", "INTEGER")
	translated = strings.ReplaceAll(translated, "TIMESTAMPTZ", "INTEGER")
	translated = strings.ReplaceAll(translated, "VARCHAR", "TEXT")
	translated = strings.ReplaceAll(translated, "DECIMAL", "REAL")

	translated = strings.ReplaceAll(translated, "::bigint", "")
	translated = strings.ReplaceAll(translated, "::int", "")
	translated = strings.ReplaceAll(translated, "::float", "")
	translated = strings.ReplaceAll(translated, "::numeric", "")

	translated = strings.ReplaceAll(translated, "pg_advisory_xact_lock", "sqlite_advisory_lock")

	translated = strings.ReplaceAll(translated, "generate_series", "generate_series_mock")
	translated = strings.ReplaceAll(translated, "unnest", "unnest_mock")

	reInterval := regexp.MustCompile(`'(\d+)\s*(days?|hours?|minutes?)'`)
	translated = reInterval.ReplaceAllStringFunc(translated, func(match string) string {
		num := reInterval.ReplaceAllString(match, "$1")
		unit := reInterval.ReplaceAllString(match, "$2")
		switch unit {
		case "day", "days":
			return num + " * 86400"
		case "hour", "hours":
			return num + " * 3600"
		case "minute", "minutes":
			return num + " * 60"
		default:
			return num
		}
	})

	translated = strings.ReplaceAll(translated, "interval", "")

	reAny := regexp.MustCompile(`=ANY\(\$(\d+)\)`)
	translated = reAny.ReplaceAllString(translated, "IN (SELECT value FROM json_each(CAST(? AS TEXT)))")

	reCastArrayInt := regexp.MustCompile(`\$(\d+)::int\[\]`)
	translated = reCastArrayInt.ReplaceAllString(translated, "?")

	reCastArrayBigInt := regexp.MustCompile(`\$(\d+)::bigint\[\]`)
	translated = reCastArrayBigInt.ReplaceAllString(translated, "?")

	reCastArrayNumeric := regexp.MustCompile(`\$(\d+)::numeric\[\]`)
	translated = reCastArrayNumeric.ReplaceAllString(translated, "?")

	reIntervalMath := regexp.MustCompile(`\((\d+)\s*\|\|\s*' minutes'\s*\)::interval`)
	translated = reIntervalMath.ReplaceAllStringFunc(translated, func(match string) string {
		num := reIntervalMath.ReplaceAllString(match, "$1")
		return num + " * 60"
	})

	return translated
}
