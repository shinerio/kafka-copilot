// Package cache implements a BOUNDED, append-only materialized view of Kafka
// log prefixes, used to accelerate repeated scans without re-pulling the same
// offsets from the broker every time.
//
// Bounding policy (the whole point of this package):
//   - MaxEntries: global cap on the number of cached messages. 0 = unlimited.
//   - MaxBytes:   global cap on the LOGICAL payload bytes stored. 0 = unlimited.
//
// Both caps are hard limits. When either is exceeded, the OLDEST entries are
// evicted first (lowest offset per partition). This keeps the "recent tail"
// that --last N queries actually hit, and is safe: anything evicted can always
// be re-fetched from the broker on demand.
//
// Physical footprint note: SQLite adds index + page overhead, so real disk usage
// is roughly 1.3-1.6x the logical MaxBytes budget. Callers should leave
// headroom and may call Compact() after large deletions to reclaim space
// (VACUUM).
package cache

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// DefaultLimits are conservative caps so the cache stays a bounded accelerator
// rather than an unbounded disk hog on the local node.
const (
	DefaultMaxEntries int64 = 2_000_000 // ~2M messages
	DefaultMaxBytes   int64 = 2 << 30   // 2 GiB logical payload budget
)

// Entry is a single cached Kafka message. It mirrors the wire fields of
// filter.Message so the scanner can hand bytes straight in without a copy.
type Entry struct {
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
}

// size returns the logical byte cost of this entry (payload only).
func (e *Entry) size() int64 {
	s := int64(len(e.Key) + len(e.Value))
	for k, v := range e.Headers {
		s += int64(len(k) + len(v))
	}
	return s
}

// Config controls the cache bounds. A zero value for either cap means
// "unlimited"; Open() substitutes the defaults in that case.
type Config struct {
	Enabled    bool
	MaxEntries int64 // global cap on cached message count (0 = unlimited)
	MaxBytes   int64 // global cap on logical payload bytes (0 = unlimited)
}

// Store is a bounded, append-only cache of Kafka log messages.
type Store struct {
	db           *sql.DB
	dbPath       string
	cfg          Config
	totalEntries int64
	totalBytes   int64
}

// Open opens (creating if needed) the cache database at path and enforces any
// pre-existing over-budget state immediately.
func Open(path string, cfg Config) (*Store, error) {
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = DefaultMaxEntries
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	db, err := sql.Open("sqlite",
		path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		return nil, fmt.Errorf("open cache db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping cache db: %w", err)
	}
	s := &Store{db: db, dbPath: path, cfg: cfg}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.loadMeta(); err != nil {
		db.Close()
		return nil, err
	}
	// An existing cache may already exceed the (possibly tightened) caps.
	if err := s.enforce(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS cache_messages (
			profile   TEXT    NOT NULL,
			topic     TEXT    NOT NULL,
			topic_id  TEXT    NOT NULL DEFAULT '',
			partition INTEGER NOT NULL,
			offset    INTEGER NOT NULL,
			ts        INTEGER NOT NULL,
			key       BLOB,
			value     BLOB,
			headers   BLOB,
			size      INTEGER NOT NULL,
			PRIMARY KEY (profile, topic, partition, offset)
		);
		CREATE TABLE IF NOT EXISTS cache_meta (
			id            INTEGER PRIMARY KEY CHECK (id = 1),
			total_entries INTEGER NOT NULL DEFAULT 0,
			total_bytes   INTEGER NOT NULL DEFAULT 0
		);
		INSERT OR IGNORE INTO cache_meta (id, total_entries, total_bytes) VALUES (1, 0, 0);
	`)
	return err
}

func (s *Store) loadMeta() error {
	row := s.db.QueryRow(`SELECT total_entries, total_bytes FROM cache_meta WHERE id = 1`)
	var e, b int64
	if err := row.Scan(&e, &b); err != nil {
		return fmt.Errorf("load cache meta: %w", err)
	}
	s.totalEntries, s.totalBytes = e, b
	return nil
}

// Append writes entries for one (profile, topic, partition). It is idempotent:
// re-writing the same offset is a no-op (ON CONFLICT DO NOTHING). Topic
// recreation is detected via topicID — if a non-empty stored topicID differs
// from the incoming one, the whole (profile, topic) set is purged first, so a
// recreated topic (offset space reset to 0) can never mix with stale data.
func (s *Store) Append(profile, topic, topicID string, partition int32, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if topicID != "" {
		var stored string
		_ = tx.QueryRow(`SELECT topic_id FROM cache_messages WHERE profile=? AND topic=? LIMIT 1`,
			profile, topic).Scan(&stored)
		if stored != "" && stored != topicID {
			if _, err := tx.Exec(`DELETE FROM cache_messages WHERE profile=? AND topic=?`,
				profile, topic); err != nil {
				return err
			}
			s.totalEntries = 0
			s.totalBytes = 0
			if _, err := tx.Exec(`UPDATE cache_meta SET total_entries=0, total_bytes=0 WHERE id=1`); err != nil {
				return err
			}
		}
	}

	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO cache_messages
		(profile, topic, topic_id, partition, offset, ts, key, value, headers, size)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var addedCount, addedBytes int64
	for _, e := range entries {
		hb, _ := json.Marshal(e.Headers)
		sz := e.size()
		res, err := stmt.Exec(profile, topic, topicID, e.Partition, e.Offset,
			e.Timestamp.UnixMilli(), e.Key, e.Value, hb, sz)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			addedCount++
			addedBytes += sz
		}
	}
	if addedCount > 0 {
		if _, err := tx.Exec(`UPDATE cache_meta SET total_entries=total_entries+?, total_bytes=total_bytes+? WHERE id=1`,
			addedCount, addedBytes); err != nil {
			return err
		}
		s.totalEntries += addedCount
		s.totalBytes += addedBytes
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.enforce()
}

// GetRange returns cached entries in [fromOffset, toOffset) for a partition,
// ordered by offset ascending. Offsets not present in the cache are simply
// absent — the caller tops them up from the broker.
func (s *Store) GetRange(profile, topic string, partition int32, fromOffset, toOffset int64) ([]Entry, error) {
	rows, err := s.db.Query(`SELECT partition, offset, ts, key, value, headers FROM cache_messages
		WHERE profile=? AND topic=? AND partition=? AND offset>=? AND offset<?
		ORDER BY offset ASC`, profile, topic, partition, fromOffset, toOffset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Entry, 0, 64)
	for rows.Next() {
		var e Entry
		var ts int64
		var hb []byte
		if err := rows.Scan(&e.Partition, &e.Offset, &ts, &e.Key, &e.Value, &hb); err != nil {
			return nil, err
		}
		e.Timestamp = time.UnixMilli(ts)
		if len(hb) > 0 {
			_ = json.Unmarshal(hb, &e.Headers)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EvictBelow removes cached entries for a partition with offset < logStart —
// i.e. data the broker has already deleted via retention or DeleteRecords.
func (s *Store) EvictBelow(profile, topic string, partition int32, logStart int64) error {
	if logStart <= 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var cnt, bs int64
	if err := tx.QueryRow(`SELECT COUNT(*), COALESCE(SUM(size),0) FROM cache_messages
		WHERE profile=? AND topic=? AND partition=? AND offset<?`,
		profile, topic, partition, logStart).Scan(&cnt, &bs); err != nil {
		return err
	}
	if cnt > 0 {
		if _, err := tx.Exec(`DELETE FROM cache_messages WHERE profile=? AND topic=? AND partition=? AND offset<?`,
			profile, topic, partition, logStart); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE cache_meta SET total_entries=total_entries-?, total_bytes=total_bytes-? WHERE id=1`,
			cnt, bs); err != nil {
			return err
		}
		s.totalEntries -= cnt
		s.totalBytes -= bs
	}
	return tx.Commit()
}

// EvictPartition drops all cached entries for a single partition (e.g. on a
// topic partition reset or explicit invalidation).
func (s *Store) EvictPartition(profile, topic string, partition int32) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var cnt, bs int64
	if err := tx.QueryRow(`SELECT COUNT(*), COALESCE(SUM(size),0) FROM cache_messages
		WHERE profile=? AND topic=? AND partition=?`,
		profile, topic, partition).Scan(&cnt, &bs); err != nil {
		return err
	}
	if cnt > 0 {
		if _, err := tx.Exec(`DELETE FROM cache_messages WHERE profile=? AND topic=? AND partition=?`,
			profile, topic, partition); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE cache_meta SET total_entries=total_entries-?, total_bytes=total_bytes-? WHERE id=1`,
			cnt, bs); err != nil {
			return err
		}
		s.totalEntries -= cnt
		s.totalBytes -= bs
	}
	return tx.Commit()
}

// Clear wipes the entire cache (e.g. the "清除缓存" button in Settings).
func (s *Store) Clear() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM cache_messages`); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE cache_meta SET total_entries=0, total_bytes=0 WHERE id=1`); err != nil {
		return err
	}
	s.totalEntries = 0
	s.totalBytes = 0
	return tx.Commit()
}

// SetLimits updates the caps at runtime (from settings) and immediately trims
// any over-budget state.
func (s *Store) SetLimits(cfg Config) error {
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = DefaultMaxEntries
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	s.cfg = cfg
	return s.enforce()
}

// enforce brings the cache back within BOTH caps by evicting the oldest entries
// first (lowest offset per partition). It loops until neither cap is exceeded.
func (s *Store) enforce() error {
	const batch = 5000
	for {
		overE := s.cfg.MaxEntries > 0 && s.totalEntries > s.cfg.MaxEntries
		overB := s.cfg.MaxBytes > 0 && s.totalBytes > s.cfg.MaxBytes
		if !overE && !overB {
			break
		}
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}

		del := int64(batch)
		if overE {
			del = s.totalEntries - s.cfg.MaxEntries
			if del > batch {
				del = batch
			}
		} else {
			// Byte cap only: delete the shortest prefix of oldest rows whose
			// total size releases enough bytes. Deleting a whole batch here
			// would wipe entries that still fit in the budget.
			need := s.totalBytes - s.cfg.MaxBytes
			rows, err := tx.Query(`SELECT size FROM cache_messages
				ORDER BY profile, topic, partition, offset ASC LIMIT ?`, batch)
			if err != nil {
				tx.Rollback()
				return err
			}
			var freed, n int64
			for rows.Next() {
				var sz int64
				if err := rows.Scan(&sz); err != nil {
					rows.Close()
					tx.Rollback()
					return err
				}
				freed += sz
				n++
				if freed >= need {
					break
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				tx.Rollback()
				return err
			}
			rows.Close()
			del = n
			if del == 0 {
				// Zero-size rows or empty table: nothing left to free.
				tx.Rollback()
				break
			}
		}
		var cnt, bs int64
		if err := tx.QueryRow(`SELECT COUNT(*), COALESCE(SUM(size),0) FROM (
			SELECT size FROM cache_messages ORDER BY profile, topic, partition, offset ASC LIMIT ?)`, del).Scan(&cnt, &bs); err != nil {
			tx.Rollback()
			return err
		}
		if cnt == 0 {
			tx.Rollback()
			break
		}
		if _, err := tx.Exec(`DELETE FROM cache_messages WHERE rowid IN (
			SELECT rowid FROM cache_messages ORDER BY profile, topic, partition, offset ASC LIMIT ?)`, cnt); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.Exec(`UPDATE cache_meta SET total_entries=total_entries-?, total_bytes=total_bytes-? WHERE id=1`,
			cnt, bs); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		s.totalEntries -= cnt
		s.totalBytes -= bs
	}
	return nil
}

// Stats reports live bounds and usage for display / diagnostics.
type Stats struct {
	Enabled       bool
	TotalEntries  int64
	TotalBytes    int64
	MaxEntries    int64
	MaxBytes      int64
	PhysicalBytes int64 // db + -wal + -shm on disk
}

// Stats returns current cache statistics.
func (s *Store) Stats() Stats {
	return Stats{
		Enabled:       s.cfg.Enabled,
		TotalEntries:  s.totalEntries,
		TotalBytes:    s.totalBytes,
		MaxEntries:    s.cfg.MaxEntries,
		MaxBytes:      s.cfg.MaxBytes,
		PhysicalBytes: physicalBytes(s.dbPath),
	}
}

// Compact reclaims disk space after large deletions (VACUUM). It is optional
// and may be called periodically or after Clear().
func (s *Store) Compact() error {
	_, err := s.db.Exec(`VACUUM`)
	return err
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// physicalBytes sums the cache db file and its WAL/shm sidecars.
func physicalBytes(path string) int64 {
	var total int64
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if fi, err := os.Stat(p); err == nil {
			total += fi.Size()
		}
	}
	return total
}

// DefaultPath returns the conventional cache db path under appData.
func DefaultPath(appData string) string {
	return filepath.Join(appData, "cache.db")
}
