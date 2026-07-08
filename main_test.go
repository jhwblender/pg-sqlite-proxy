package main

import (
	"strings"
	"testing"
)

// --- DROP ... CASCADE -------------------------------------------------

func TestTranslateSQL_DropTableCascade(t *testing.T) {
	cases := []struct{ in, want string }{
		{`DROP TABLE IF EXISTS "Transaction" CASCADE`, `DROP TABLE IF EXISTS "Transaction"`},
		{`DROP TABLE "User" CASCADE`, `DROP TABLE "User"`},
		{`DROP VIEW my_view RESTRICT`, `DROP VIEW my_view`},
		{`DROP INDEX idx_foo CASCADE`, `DROP INDEX idx_foo`},
		{`DROP TABLE IF EXISTS "Transaction"`, `DROP TABLE IF EXISTS "Transaction"`}, // unchanged
	}
	for _, c := range cases {
		got := translateSQL(c.in)
		if got != c.want {
			t.Errorf("translateSQL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// --- CREATE TABLE IF NOT EXISTS ---------------------------------------

func TestTranslateSQL_CreateTableIfNotExistsPreserved(t *testing.T) {
	in := `CREATE TABLE IF NOT EXISTS "User" (id TEXT PRIMARY KEY)`
	got := translateSQL(in)
	if strings.Contains(got, "ADD COLUMN") {
		t.Fatalf("CREATE TABLE IF NOT EXISTS got corrupted into an ADD COLUMN statement: %q", got)
	}
	if !strings.Contains(got, "IF NOT EXISTS") {
		t.Fatalf("CREATE TABLE IF NOT EXISTS clause was stripped instead of preserved (SQLite supports it natively): %q", got)
	}
}

func TestTranslateSQL_AddColumnIfNotExistsStripped(t *testing.T) {
	in := `ALTER TABLE "User" ADD COLUMN IF NOT EXISTS foo INTEGER`
	got := translateSQL(in)
	if strings.Contains(got, "IF NOT EXISTS") {
		t.Fatalf("ADD COLUMN IF NOT EXISTS should have IF NOT EXISTS stripped (SQLite has no such clause): %q", got)
	}
	if strings.Contains(got, "ADD COLUMN ADD COLUMN") {
		t.Fatalf("ADD COLUMN got duplicated: %q", got)
	}
}

// --- ENUM support -------------------------------------------------------

func TestTranslateSQL_EnumCreateTypeIsNoOp(t *testing.T) {
	knownEnumTypesMu.Lock()
	knownEnumTypes = make(map[string]bool)
	knownEnumTypesMu.Unlock()

	got := translateSQL(`CREATE TYPE "RecurrenceType" AS ENUM ('MONTHLY','WEEKLY')`)
	if got != "" {
		t.Fatalf("CREATE TYPE ... AS ENUM should translate to a no-op, got %q", got)
	}

	knownEnumTypesMu.Lock()
	_, ok := knownEnumTypes["RecurrenceType"]
	knownEnumTypesMu.Unlock()
	if !ok {
		t.Fatal("enum type name was not registered")
	}
}

func TestTranslateSQL_EnumColumnBecomesText(t *testing.T) {
	knownEnumTypesMu.Lock()
	knownEnumTypes = make(map[string]bool)
	knownEnumTypesMu.Unlock()
	registerEnumType("RecurrenceType")

	got := translateSQL(`CREATE TABLE "Regular" (id TEXT PRIMARY KEY, "recurrenceType" "RecurrenceType" NOT NULL)`)
	if strings.Contains(got, "RecurrenceType") {
		t.Fatalf("enum type reference should have been replaced with TEXT: %q", got)
	}
	if !strings.Contains(got, `"recurrenceType" TEXT NOT NULL`) {
		t.Fatalf("expected the column to become TEXT, got: %q", got)
	}
}

func TestTranslateSQL_DropAndAlterTypeAreNoOps(t *testing.T) {
	if got := translateSQL(`DROP TYPE "RecurrenceType"`); got != "" {
		t.Errorf("DROP TYPE should be a no-op, got %q", got)
	}
	if got := translateSQL(`ALTER TYPE "RecurrenceType" ADD VALUE 'BIWEEKLY'`); got != "" {
		t.Errorf("ALTER TYPE should be a no-op, got %q", got)
	}
}

// --- TIMESTAMP mapping ---------------------------------------------------

func TestTranslateSQL_PlainTimestampBecomesText(t *testing.T) {
	got := translateSQL(`CREATE TABLE t (id TEXT PRIMARY KEY, "createdAt" TIMESTAMP NOT NULL)`)
	if strings.Contains(got, "TIMESTAMP") {
		t.Fatalf("plain TIMESTAMP should be mapped to TEXT (to avoid the sqlite driver's time.Time auto-conversion), got: %q", got)
	}
	if !strings.Contains(got, `"createdAt" TEXT NOT NULL`) {
		t.Fatalf("expected TEXT column, got: %q", got)
	}
}

func TestTranslateSQL_TimestamptzStillBecomesText(t *testing.T) {
	got := translateSQL(`CREATE TABLE t (id TEXT PRIMARY KEY, d TIMESTAMPTZ NOT NULL)`)
	if strings.Contains(got, "TIMESTAMPTZ") || strings.Contains(got, "TIMESTAMP") {
		t.Fatalf("TIMESTAMPTZ should map to TEXT: %q", got)
	}
}

// --- Timestamp parameter normalization ------------------------------------

func TestNormalizeTimestampParam(t *testing.T) {
	// node-postgres serializes a bound Date parameter with an explicit
	// local-timezone offset, not UTC/"Z". Both must normalize to the same
	// UTC, second-precision text that SQLite's own CURRENT_TIMESTAMP uses.
	cases := []struct {
		in   string
		want string
	}{
		{"2026-07-08T18:33:02.370-05:00", "2026-07-08 23:33:02"},
		{"2026-07-08T23:33:02Z", "2026-07-08 23:33:02"},
		{"2026-07-08T23:33:02.999999999Z", "2026-07-08 23:33:02"},
	}
	for _, c := range cases {
		got := normalizeTimestampParam(c.in)
		if got != c.want {
			t.Errorf("normalizeTimestampParam(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNormalizeTimestampParam_NonTimestampUnchanged(t *testing.T) {
	// Ordinary string/number parameters must not be touched.
	cases := []string{
		"hello world",
		"c000000000000000000000001",
		"a@example.com",
		"42",
		"true",
	}
	for _, in := range cases {
		if got := normalizeTimestampParam(in); got != in {
			t.Errorf("normalizeTimestampParam(%q) = %q, want unchanged", in, got)
		}
	}
}

func TestNormalizeTimestampParam_ConsistentWithSQLiteDefault(t *testing.T) {
	// The whole point of normalization: a bound Date parameter and
	// SQLite's own CURRENT_TIMESTAMP output must be textually comparable.
	// This mirrors the exact instant represented by
	// "2026-07-08T18:33:02-05:00" (America/Chicago, CDT) in UTC.
	got := normalizeTimestampParam("2026-07-08T18:33:02-05:00")
	want := "2026-07-08 23:33:02" // same format CURRENT_TIMESTAMP would produce
	if got != want {
		t.Errorf("normalizeTimestampParam(...) = %q, want %q (format must match CURRENT_TIMESTAMP's own output)", got, want)
	}
}

// --- BEGIN/COMMIT/ROLLBACK must actually execute, not no-op --------------

func TestTxControlCommand(t *testing.T) {
	cases := []struct {
		in      string
		wantCmd string
		wantOk  bool
	}{
		{"BEGIN", "BEGIN", true},
		{"BEGIN TRANSACTION", "BEGIN", true},
		{"START TRANSACTION", "BEGIN", true},
		{"COMMIT", "COMMIT", true},
		{"END", "COMMIT", true},
		{"ROLLBACK", "ROLLBACK", true},
		{"SELECT 1", "", false},
		{"SET timezone = 'UTC'", "", false},
	}
	for _, c := range cases {
		cmd, ok := txControlCommand(c.in)
		if ok != c.wantOk || cmd != c.wantCmd {
			t.Errorf("txControlCommand(%q) = (%q, %v), want (%q, %v)", c.in, cmd, ok, c.wantCmd, c.wantOk)
		}
	}
}

func TestIsNoOp_DoesNotSwallowTxControl(t *testing.T) {
	// Regression guard: BEGIN/COMMIT/ROLLBACK must NOT be treated as
	// generic no-ops (that was the bug — they were acknowledged without
	// ever touching the database, so ROLLBACK never rolled anything back).
	for _, cmd := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
		if isNoOp(cmd) {
			t.Errorf("isNoOp(%q) = true; BEGIN/COMMIT/ROLLBACK must be handled by txControlCommand, not treated as a no-op", cmd)
		}
	}
	// SET/SHOW genuinely have no SQLite equivalent and should remain no-ops.
	if !isNoOp("SET timezone = 'UTC'") {
		t.Error(`isNoOp("SET timezone = 'UTC'") = false, want true`)
	}
}
