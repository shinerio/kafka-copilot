package audit

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Op represents an operation audit record.
type Op struct {
	OpID          string `json:"op_id"`
	SessionID     string `json:"session_id,omitempty"`
	Source        string `json:"source"` // ai/human
	Actor         string `json:"actor"`
	Intent        string `json:"intent,omitempty"`
	Command       string `json:"command"`
	CommandTrunc  bool   `json:"commandTruncated,omitempty"`
	StartedAt     int64  `json:"started_at"`
	DurationMs    int64  `json:"duration_ms"`
	Status        string `json:"status"` // running/ok/error/truncated/cancelled/crashed
	ResultSummary string `json:"result_summary,omitempty"`
	Warnings      string `json:"warnings,omitempty"`
	Error         string `json:"error,omitempty"`
}

// Session represents a session record.
type Session struct {
	SessionID string `json:"session_id"`
	Label     string `json:"label,omitempty"`
	StartedAt int64  `json:"started_at"`
	EndedAt   int64  `json:"ended_at,omitempty"`
	OpCount   int    `json:"op_count"`
	Auto      bool   `json:"auto,omitempty"`
}

// Snapshot holds bounded scan hit messages for local replay.
type Snapshot struct {
	OpID      string          `json:"op_id"`
	SessionID string          `json:"session_id,omitempty"`
	CreatedAt int64           `json:"created_at"`
	Messages  json.RawMessage `json:"messages"`
	Truncated bool            `json:"truncated"`
}

// Store manages audit data in SQLite.
type Store struct {
	db *sql.DB
}

// Open opens or creates the audit database at the given path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		return nil, fmt.Errorf("open audit db: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping audit db: %w", err)
	}
	s := &Store{db: db}
	if err := s.init(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) init() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS operations (
			op_id          TEXT PRIMARY KEY,
			session_id     TEXT,
			source         TEXT NOT NULL DEFAULT 'ai',
			actor          TEXT,
			intent         TEXT,
			command        TEXT,
			command_trunc  INTEGER DEFAULT 0,
			started_at     INTEGER NOT NULL,
			duration_ms    INTEGER DEFAULT 0,
			status         TEXT NOT NULL DEFAULT 'running',
			result_summary TEXT,
			warnings       TEXT,
			error          TEXT
		);
		CREATE TABLE IF NOT EXISTS sessions (
			session_id TEXT PRIMARY KEY,
			label      TEXT,
			started_at INTEGER NOT NULL,
			ended_at   INTEGER,
			op_count   INTEGER DEFAULT 0,
			auto       INTEGER DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS snapshots (
			op_id      TEXT PRIMARY KEY,
			session_id TEXT,
			created_at INTEGER NOT NULL,
			messages   TEXT,
			truncated  INTEGER DEFAULT 0,
			FOREIGN KEY (op_id) REFERENCES operations(op_id) ON DELETE CASCADE
		);
		CREATE INDEX IF NOT EXISTS idx_ops_session ON operations(session_id, started_at);
		CREATE INDEX IF NOT EXISTS idx_ops_source ON operations(source, started_at);
		CREATE INDEX IF NOT EXISTS idx_ops_status ON operations(status);
		CREATE INDEX IF NOT EXISTS idx_snaps_session ON snapshots(session_id);
	`)
	if err != nil {
		return fmt.Errorf("init tables: %w", err)
	}
	return nil
}

// StartOp inserts a new op with status=running (dual-phase commit, FR-09).
func (s *Store) StartOp(op *Op) error {
	_, err := s.db.Exec(`
		INSERT INTO operations (op_id, session_id, source, actor, intent, command, command_trunc, started_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running')
	`,
		op.OpID, op.SessionID, op.Source, op.Actor, op.Intent,
		op.Command, boolToInt(op.CommandTrunc), op.StartedAt,
	)
	if err != nil {
		return fmt.Errorf("insert op: %w", err)
	}
	// Update session op_count if session exists
	if op.SessionID != "" {
		s.incSessionOpCount(op.SessionID)
	}
	return nil
}

// FinishOp updates the op to its terminal state (dual-phase commit).
func (s *Store) FinishOp(opID string, status string, durationMs int64, resultSummary string, warnings string, opError string) error {
	_, err := s.db.Exec(`
		UPDATE operations
		SET status = ?, duration_ms = ?, result_summary = ?, warnings = ?, error = ?
		WHERE op_id = ?
	`, status, durationMs, resultSummary, warnings, opError, opID)
	if err != nil {
		return fmt.Errorf("update op: %w", err)
	}
	return nil
}

// UpdateOpProgress updates result_summary in-place (for produce progress, FR-09 rule 3).
func (s *Store) UpdateOpProgress(opID string, resultSummary string) error {
	_, err := s.db.Exec(`UPDATE operations SET result_summary = ? WHERE op_id = ?`, resultSummary, opID)
	if err != nil {
		return fmt.Errorf("update op progress: %w", err)
	}
	return nil
}

// GetOp retrieves a single op by ID.
func (s *Store) GetOp(opID string) (*Op, error) {
	row := s.db.QueryRow(`
		SELECT op_id, session_id, source, actor, intent, command, command_trunc,
		       started_at, duration_ms, status, result_summary, warnings, error
		FROM operations WHERE op_id = ?
	`, opID)
	return scanOp(row)
}

// ListOps lists ops with optional filters.
func (s *Store) ListOps(sessionID string, source string, since int64, limit int) ([]*Op, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	q := `SELECT op_id, session_id, source, actor, intent, command, command_trunc,
	             started_at, duration_ms, status, result_summary, warnings, error
	      FROM operations WHERE 1=1`
	var args []interface{}
	if sessionID != "" {
		q += " AND session_id = ?"
		args = append(args, sessionID)
	}
	if source != "" {
		q += " AND source = ?"
		args = append(args, source)
	}
	if since > 0 {
		q += " AND started_at >= ?"
		args = append(args, since)
	}
	q += " ORDER BY started_at DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("list ops: %w", err)
	}
	defer rows.Close()
	var ops []*Op
	for rows.Next() {
		op, err := scanOp(rows)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

// SaveSnapshot saves bounded scan hit messages (FR-09 rule 4).
func (s *Store) SaveSnapshot(snap *Snapshot) error {
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO snapshots (op_id, session_id, created_at, messages, truncated)
		VALUES (?, ?, ?, ?, ?)
	`, snap.OpID, snap.SessionID, snap.CreatedAt, string(snap.Messages), boolToInt(snap.Truncated))
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	return nil
}

// GetSnapshot retrieves a snapshot by op_id.
func (s *Store) GetSnapshot(opID string) (*Snapshot, error) {
	row := s.db.QueryRow(`
		SELECT op_id, session_id, created_at, messages, truncated
		FROM snapshots WHERE op_id = ?
	`, opID)
	var snap Snapshot
	var messages string
	var sessionID sql.NullString
	var truncated int
	if err := row.Scan(&snap.OpID, &sessionID, &snap.CreatedAt, &messages, &truncated); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	snap.SessionID = sessionID.String
	snap.Messages = json.RawMessage(messages)
	snap.Truncated = truncated == 1
	return &snap, nil
}

// CreateSession creates a new session.
func (s *Store) CreateSession(sessionID string, label string, startedAt int64) error {
	_, err := s.db.Exec(`
		INSERT INTO sessions (session_id, label, started_at, op_count, auto)
		VALUES (?, ?, ?, 0, 0)
	`, sessionID, label, startedAt)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	return nil
}

// EndSession marks a session as ended.
func (s *Store) EndSession(sessionID string, endedAt int64, auto bool) error {
	_, err := s.db.Exec(`
		UPDATE sessions SET ended_at = ?, auto = ? WHERE session_id = ?
	`, endedAt, boolToInt(auto), sessionID)
	if err != nil {
		return fmt.Errorf("end session: %w", err)
	}
	return nil
}

// ReopenSession clears ended_at so an idle session becomes live again
// (used to keep the catch-all default session always open for the CLI log).
func (s *Store) ReopenSession(sessionID string) error {
	_, err := s.db.Exec(`
		UPDATE sessions SET ended_at = NULL, auto = 0 WHERE session_id = ?
	`, sessionID)
	if err != nil {
		return fmt.Errorf("reopen session: %w", err)
	}
	return nil
}

// ListSessions lists all sessions.
func (s *Store) ListSessions(limit int) ([]*Session, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.Query(`
		SELECT session_id, label, started_at, ended_at, op_count, auto
		FROM sessions ORDER BY started_at DESC LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}
	defer rows.Close()
	var sessions []*Session
	for rows.Next() {
		var sess Session
		var label sql.NullString
		var endedAt sql.NullInt64
		var auto int
		if err := rows.Scan(&sess.SessionID, &label, &sess.StartedAt, &endedAt, &sess.OpCount, &auto); err != nil {
			return nil, err
		}
		sess.Label = label.String
		sess.EndedAt = endedAt.Int64
		sess.Auto = auto == 1
		sessions = append(sessions, &sess)
	}
	return sessions, rows.Err()
}

// GetSession retrieves a session by ID.
func (s *Store) GetSession(sessionID string) (*Session, error) {
	row := s.db.QueryRow(`
		SELECT session_id, label, started_at, ended_at, op_count, auto
		FROM sessions WHERE session_id = ?
	`, sessionID)
	var sess Session
	var label sql.NullString
	var endedAt sql.NullInt64
	var auto int
	if err := row.Scan(&sess.SessionID, &label, &sess.StartedAt, &endedAt, &sess.OpCount, &auto); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	sess.Label = label.String
	sess.EndedAt = endedAt.Int64
	sess.Auto = auto == 1
	return &sess, nil
}

func placeholders(n int) string {
	q := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			q += ","
		}
		q += "?"
	}
	return q
}

func toAnySlice(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, v := range ss {
		out[i] = v
	}
	return out
}

// DeleteOps removes selected operations and their snapshots inside one
// transaction, then recalculates op_count for every affected session.
func (s *Store) DeleteOps(opIDs []string) (int64, error) {
	if len(opIDs) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	ph := placeholders(len(opIDs))
	args := toAnySlice(opIDs)

	// Collect affected sessions before deleting.
	rows, err := tx.Query(
		`SELECT DISTINCT session_id FROM operations WHERE op_id IN (`+ph+`)
		 AND session_id IS NOT NULL AND session_id <> ''`, args...)
	if err != nil {
		return 0, err
	}
	var sessions []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			sessions = append(sessions, id)
		}
	}
	rows.Close()

	if _, err := tx.Exec(`DELETE FROM snapshots WHERE op_id IN (`+ph+`)`, args...); err != nil {
		return 0, err
	}
	res, err := tx.Exec(`DELETE FROM operations WHERE op_id IN (`+ph+`)`, args...)
	if err != nil {
		return 0, err
	}
	deleted, _ := res.RowsAffected()

	for _, sid := range sessions {
		if _, err := tx.Exec(
			`UPDATE sessions SET op_count = (SELECT COUNT(*) FROM operations WHERE session_id = ?)
			 WHERE session_id = ?`, sid, sid); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

// DeleteSessions removes selected sessions together with all their ops and
// snapshots, inside one transaction.
func (s *Store) DeleteSessions(sessionIDs []string) (int64, error) {
	if len(sessionIDs) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	ph := placeholders(len(sessionIDs))
	args := toAnySlice(sessionIDs)

	if _, err := tx.Exec(
		`DELETE FROM snapshots WHERE session_id IN (`+ph+`)
		 OR op_id IN (SELECT op_id FROM operations WHERE session_id IN (`+ph+`))`,
		append(append([]interface{}{}, args...), args...)...); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`DELETE FROM operations WHERE session_id IN (`+ph+`)`, args...); err != nil {
		return 0, err
	}
	res, err := tx.Exec(`DELETE FROM sessions WHERE session_id IN (`+ph+`)`, args...)
	if err != nil {
		return 0, err
	}
	deleted, _ := res.RowsAffected()
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

// ClearAudit clears audit data optionally before a timestamp (FR-09 rule 6).
func (s *Store) ClearAudit(before int64) error {
	if before > 0 {
		_, err := s.db.Exec(`DELETE FROM snapshots WHERE created_at < ?`, before)
		if err != nil {
			return err
		}
		_, err = s.db.Exec(`DELETE FROM operations WHERE started_at < ?`, before)
		if err != nil {
			return err
		}
		_, err = s.db.Exec(`DELETE FROM sessions WHERE started_at < ? AND ended_at IS NOT NULL`, before)
		return err
	}
	// Clear all
	_, err := s.db.Exec(`DELETE FROM snapshots`)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`DELETE FROM operations`)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`DELETE FROM sessions`)
	return err
}

// CleanupRetentionPolicy enforces 30-day / 500MB retention (FR-09, §11.3).
func (s *Store) CleanupRetentionPolicy(maxAgeDays int, maxBytes int64) error {
	if maxAgeDays <= 0 {
		maxAgeDays = 30
	}
	cutoff := time.Now().AddDate(0, 0, -maxAgeDays).UnixMilli()
	// Delete old sessions and their ops/snapshots
	_, err := s.db.Exec(`DELETE FROM snapshots WHERE created_at < ?`, cutoff)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`DELETE FROM operations WHERE started_at < ?`, cutoff)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`DELETE FROM sessions WHERE started_at < ? AND ended_at IS NOT NULL`, cutoff)
	if err != nil {
		return err
	}

	// Size-based cleanup
	if maxBytes > 0 {
		var totalBytes int64
		row := s.db.QueryRow(`SELECT SUM(LENGTH(messages)) + SUM(LENGTH(result_summary)) + SUM(LENGTH(command)) FROM operations LEFT JOIN snapshots ON operations.op_id = snapshots.op_id`)
		row.Scan(&totalBytes)
		if totalBytes > maxBytes {
			// Delete oldest 10% of sessions
			_, err = s.db.Exec(`
				DELETE FROM operations WHERE session_id IN (
					SELECT session_id FROM sessions ORDER BY started_at ASC LIMIT 1
				)
			`)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// RecoverCrashedOps marks ops stuck in running > 10 min as crashed (FR-09 rule 2).
func (s *Store) RecoverCrashedOps() (int, error) {
	cutoff := time.Now().Add(-10 * time.Minute).UnixMilli()
	res, err := s.db.Exec(`
		UPDATE operations SET status = 'crashed', error = '{"code":"crashed","message":"process may have been killed"}'
		WHERE status = 'running' AND started_at < ?
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("recover crashed ops: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// AutoEndSessions ends sessions with no new ops in 24h (FR-10 rule 3).
func (s *Store) AutoEndSessions() (int, error) {
	cutoff := time.Now().Add(-24 * time.Hour).UnixMilli()
	res, err := s.db.Exec(`
		UPDATE sessions SET ended_at = ?, auto = 1
		WHERE ended_at IS NULL AND session_id IN (
			SELECT s.session_id FROM sessions s
			LEFT JOIN (SELECT session_id, MAX(started_at) as last_op FROM operations GROUP BY session_id) o
			ON s.session_id = o.session_id
			WHERE COALESCE(o.last_op, s.started_at) < ?
		)
	`, time.Now().UnixMilli(), cutoff)
	if err != nil {
		return 0, fmt.Errorf("auto end sessions: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) incSessionOpCount(sessionID string) {
	s.db.Exec(`UPDATE sessions SET op_count = op_count + 1 WHERE session_id = ?`, sessionID)
}

// Helper: scan op from rows or row
type scanner interface {
	Scan(dest ...interface{}) error
}

func scanOp(sc scanner) (*Op, error) {
	var op Op
	var sessionID, intent, resultSummary, warnings, opError sql.NullString
	var commandTrunc int
	if err := sc.Scan(
		&op.OpID, &sessionID, &op.Source, &op.Actor, &intent, &op.Command, &commandTrunc,
		&op.StartedAt, &op.DurationMs, &op.Status, &resultSummary, &warnings, &opError,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("op not found")
		}
		return nil, err
	}
	op.SessionID = sessionID.String
	op.Intent = intent.String
	op.CommandTrunc = commandTrunc == 1
	op.ResultSummary = resultSummary.String
	op.Warnings = warnings.String
	op.Error = opError.String
	return &op, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// GenerateSessionID generates a 6-char Crockford Base32 session ID (FR-10).
func GenerateSessionID() string {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ" // Crockford Base32 (no I, L, O, U)
	b := make([]byte, 6)
	now := time.Now().UnixNano()
	for i := range b {
		b[i] = alphabet[(now>>uint(i*5))&31]
	}
	// Add some randomness
	t := time.Now().UnixNano()
	for i := range b {
		b[i] = alphabet[(t>>uint(i*3))&31^int64(b[i])&31]
	}
	return strings.ToUpper(string(b))
}
