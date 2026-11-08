package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultDataDirName is the portable data directory created next to kp.
	DefaultDataDirName = "kp-data"
	// SettingsFileName lives next to the executable. Its location must be
	// fixed: it is the only file that tells kp where the (movable) data dir is.
	SettingsFileName = "kp-settings.yaml"

	configFileName = "config.yaml"
	auditDBName    = "audit.db"
)

// SQLite WAL companions must travel together with audit.db. Copying a stray
// -wal/-shm into a directory whose db was NOT copied produces a corrupt
// database, so these three files are always handled as one group.
var auditDBGroup = []string{auditDBName, auditDBName + "-wal", auditDBName + "-shm"}

// ExeDir returns the directory containing the running executable. Falls back to
// the working directory if the executable path cannot be determined.
func ExeDir() string {
	if exe, err := os.Executable(); err == nil && exe != "" {
		return filepath.Dir(exe)
	}
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return "."
}

// DefaultDataDir returns the default data directory: "kp-data" next to the
// running executable. Profiles (config.yaml), audit.db and cached data live
// here so they travel with the binary instead of hiding in the OS app-data dir.
func DefaultDataDir() string {
	return filepath.Join(ExeDir(), DefaultDataDirName)
}

// SettingsFile returns the fixed path of kp-settings.yaml (next to kp.exe).
// KP_SETTINGS can override it (advanced use / tests); a settings file inside
// the data directory it points at would be unfindable, so it stays outside.
func SettingsFile() string {
	if v := os.Getenv("KP_SETTINGS"); v != "" {
		return v
	}
	return filepath.Join(ExeDir(), SettingsFileName)
}

// Settings holds user-configurable runtime paths plus the one-time migration
// marker. It is deliberately stored next to the executable, NOT inside the
// data directory, so that moving the data directory never loses the pointer.
type Settings struct {
	// DataDir is the root directory for config.yaml (profiles) and audit.db.
	// Empty means "use DefaultDataDir()". Relative paths resolve against ExeDir().
	DataDir string `yaml:"data_dir" json:"data_dir"`
	// MigratedFrom / MigratedAt record the one-time legacy -> data dir move.
	MigratedFrom string `yaml:"migrated_from,omitempty" json:"migrated_from,omitempty"`
	MigratedAt   string `yaml:"migrated_at,omitempty" json:"migrated_at,omitempty"`
	// MigrationDone makes the automatic migration strictly one-time. Set even
	// when there was nothing to move, so we never rescan the legacy dir.
	MigrationDone bool `yaml:"migration_done,omitempty" json:"migration_done,omitempty"`
}

// LoadSettings returns the current settings. When no settings file exists yet,
// it persists the defaults so the choice is stable across runs.
func LoadSettings() (*Settings, error) {
	p := SettingsFile()
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			s := &Settings{DataDir: DefaultDataDir()}
			if werr := SaveSettings(s); werr != nil {
				// Non-fatal: keep the default in memory even if we cannot write.
				return s, nil
			}
			return s, nil
		}
		return nil, fmt.Errorf("read settings: %w", err)
	}
	var s Settings
	if err := yaml.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parse settings yaml: %w", err)
	}
	if s.DataDir == "" {
		s.DataDir = DefaultDataDir()
	}
	return &s, nil
}

// SaveSettings writes kp-settings.yaml (creating its parent dir as needed).
func SaveSettings(s *Settings) error {
	if s.DataDir == "" {
		s.DataDir = DefaultDataDir()
	}
	dir := filepath.Dir(SettingsFile())
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create settings dir: %w", err)
	}
	b, err := yaml.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	if err := os.WriteFile(SettingsFile(), b, 0o600); err != nil {
		return fmt.Errorf("write settings: %w", err)
	}
	return nil
}

// ResolveDataDir normalizes a configured data dir:
//   - empty -> DefaultDataDir()
//   - absolute -> as-is
//   - relative -> resolved against the executable directory (portable)
func ResolveDataDir(dir string) string {
	dir = filepath.Clean(filepath.FromSlash(dir))
	if dir == "" || dir == "." {
		return DefaultDataDir()
	}
	if filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(ExeDir(), dir)
}

// Layout is the single, shared description of "where kp stores its data".
// CLI (`kp settings show`) and REST (`GET /api/settings`) both render this
// struct, so the two ends can never disagree about the active directory.
type Layout struct {
	DataDir                 string `json:"data_dir"`
	DefaultDataDir          string `json:"default_data_dir"`
	LegacyDir               string `json:"legacy_dir"`
	SettingsPath            string `json:"settings_path"`
	ConfigPath              string `json:"config_path"`
	AuditPath               string `json:"audit_path"`
	ConfigExists            bool   `json:"config_exists"`
	AuditExists             bool   `json:"audit_exists"`
	LegacyConfigExists      bool   `json:"legacy_config_exists"`
	LegacyAuditExists       bool   `json:"legacy_audit_exists"`
	MigratedFrom            string `json:"migrated_from"`
	MigratedAt              string `json:"migrated_at"`
	MigrationDone           bool   `json:"migration_done"`
	EnvOverride             string `json:"env_override,omitempty"`
	RestartRequiredOnChange bool   `json:"restart_required_on_change"`
}

// DataLayout resolves the active layout without mutating anything (read-only).
func DataLayout() (*Layout, error) {
	s, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	dir := ResolveDataDir(s.DataDir)
	env := os.Getenv("KP_APPDATA")
	if env != "" {
		dir = ResolveDataDir(env)
	}
	legacy := LegacyAppDataDir()
	return &Layout{
		DataDir:                 dir,
		DefaultDataDir:          DefaultDataDir(),
		LegacyDir:               legacy,
		SettingsPath:            SettingsFile(),
		ConfigPath:              filepath.Join(dir, configFileName),
		AuditPath:               filepath.Join(dir, auditDBName),
		ConfigExists:            fileExists(filepath.Join(dir, configFileName)),
		AuditExists:             fileExists(filepath.Join(dir, auditDBName)),
		LegacyConfigExists:      fileExists(filepath.Join(legacy, configFileName)),
		LegacyAuditExists:       fileExists(filepath.Join(legacy, auditDBName)),
		MigratedFrom:            s.MigratedFrom,
		MigratedAt:              s.MigratedAt,
		MigrationDone:           s.MigrationDone,
		EnvOverride:             env,
		RestartRequiredOnChange: true,
	}, nil
}

// DataDir resolves the active data directory, runs the one-time legacy
// migration when needed, and ensures the directory exists. Priority:
//  1. KP_APPDATA env (test override; skips migration because src == dest)
//  2. data_dir from kp-settings.yaml
//  3. DefaultDataDir() (kp-data next to the executable)
func DataDir() (string, error) {
	if v := os.Getenv("KP_APPDATA"); v != "" {
		dir := ResolveDataDir(v)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("create data dir %s: %w", dir, err)
		}
		return dir, nil
	}

	settings, err := LoadSettings()
	if err != nil {
		return "", err
	}
	dir := ResolveDataDir(settings.DataDir)

	if !settings.MigrationDone {
		if res, merr := MigrateData(dir, false); merr != nil {
			// Non-fatal: the app can still start fresh in the new location.
			fmt.Fprintf(os.Stderr, "warn: migrate legacy data: %v\n", merr)
		} else if res != nil && !res.AlreadyDone {
			settings.MigratedFrom = res.Source
			settings.MigratedAt = time.Now().Format(time.RFC3339)
			settings.MigrationDone = true
			if serr := SaveSettings(settings); serr != nil {
				fmt.Fprintf(os.Stderr, "warn: persist migration marker: %v\n", serr)
			}
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create data dir %s: %w", dir, err)
	}
	return dir, nil
}

// LegacyAppDataDir returns the OLD fixed app-data directory. It is used only
// as the one-time migration source and as a pid-file fallback.
func LegacyAppDataDir() string {
	if v := os.Getenv("KP_APPDATA"); v != "" {
		return v
	}
	if runtime.GOOS == "windows" {
		base := os.Getenv("APPDATA")
		if base == "" {
			base = filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming")
		}
		return filepath.Join(base, "kafka-pilot")
	}
	return filepath.Join(os.Getenv("HOME"), ".kafka-pilot")
}

// MigrationResult is the shared CLI/REST shape for a data move.
type MigrationResult struct {
	Source          string   `json:"source"`
	Dest            string   `json:"dest"`
	Copied          []string `json:"copied"`
	Skipped         []string `json:"skipped"`
	AlreadyDone     bool     `json:"already_done"`
	RestartRequired bool     `json:"restart_required"`
	Note            string   `json:"note,omitempty"`
}

// MigrateData copies any missing data files from the legacy app-data location
// into dest. It is a ONE-TIME, non-destructive MOVE-by-copy:
//   - files already present in dest are never overwritten
//   - audit.db + -wal + -shm are copied as one atomic group
//   - source files are intentionally kept (rollback is always possible)
//   - a marker in kp-settings.yaml prevents any later automatic repeat
//
// force ignores the marker (still never overwrites existing dest files).
func MigrateData(dest string, force bool) (*MigrationResult, error) {
	s, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	if dest == "" {
		dest = ResolveDataDir(s.DataDir)
	}
	src := LegacyAppDataDir()
	res := &MigrationResult{
		Source: src, Dest: dest, RestartRequired: true,
		Copied: []string{}, Skipped: []string{},
	}

	if s.MigrationDone && !force {
		res.AlreadyDone = true
		res.Note = "legacy migration already recorded; use --force to re-check"
		return res, nil
	}
	if src == dest {
		res.AlreadyDone = true
		res.Note = "KP_APPDATA is set: source and destination are the same directory"
		return res, nil
	}
	copied, skipped, err := copyLayout(src, dest)
	if err != nil {
		return nil, err
	}
	res.Copied, res.Skipped = copied, skipped

	if !s.MigrationDone {
		s.MigratedFrom = src
		s.MigratedAt = time.Now().Format(time.RFC3339)
		s.MigrationDone = true
		if err := SaveSettings(s); err != nil {
			return nil, fmt.Errorf("persist migration marker: %w", err)
		}
	}
	if len(copied) == 0 {
		res.Note = "nothing to move (no legacy data, or destination already populated)"
	}
	return res, nil
}

// CopyDataDir copies the current data files into a newly configured directory.
// Used by `kp settings set-data-dir` / `PUT /api/settings` so switching
// directories does not silently drop all profiles and audit history.
func CopyDataDir(from, to string) (*MigrationResult, error) {
	if from == "" || to == "" {
		return nil, fmt.Errorf("both source and destination are required")
	}
	if filepath.Clean(from) == filepath.Clean(to) {
		return &MigrationResult{Source: from, Dest: to, AlreadyDone: true,
			Copied: []string{}, Skipped: []string{},
			Note: "destination is the current data directory", RestartRequired: true}, nil
	}
	copied, skipped, err := copyLayout(from, to)
	if err != nil {
		return nil, err
	}
	return &MigrationResult{
		Source: from, Dest: to, Copied: copied, Skipped: skipped,
		RestartRequired: true,
		Note:            "source files were kept; restart the service to use the new directory",
	}, nil
}

// UpdateResult is the shared CLI/REST shape for a data-directory change.
type UpdateResult struct {
	Status          string   `json:"status"`
	DataDir         string   `json:"data_dir"`
	Configured      string   `json:"configured"`
	PreviousDataDir string   `json:"previous_data_dir"`
	SettingsPath    string   `json:"settings_path"`
	Copied          []string `json:"copied"`
	Skipped         []string `json:"skipped"`
	RestartRequired bool     `json:"restart_required"`
	Hint            string   `json:"hint"`
}

// SetDataDir validates and persists a new data directory. CLI and REST both
// call this single use case, so `kp settings set-data-dir` and
// `PUT /api/settings` can never diverge in validation or output.
//
// currentDir is the directory the caller is actually using (REST passes the
// running service's app.DataDir; CLI may pass "" to use the configured one).
// copyCurrent carries profiles + audit data over to the new directory so
// switching directories never silently empties the profile list.
func SetDataDir(value, currentDir string, copyCurrent bool) (*UpdateResult, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("data_dir is required")
	}
	s, err := LoadSettings()
	if err != nil {
		return nil, err
	}
	if currentDir == "" {
		currentDir = ResolveDataDir(s.DataDir)
	}
	resolved := ResolveDataDir(value)
	if err := os.MkdirAll(resolved, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create data dir %s: %w", resolved, err)
	}
	// Probe writability before committing the setting.
	probe := filepath.Join(resolved, ".kp-write-probe")
	if err := os.WriteFile(probe, []byte("ok"), 0o600); err != nil {
		return nil, fmt.Errorf("data dir not writable: %w", err)
	}
	_ = os.Remove(probe)

	res := &UpdateResult{
		Status: "saved", DataDir: resolved, Configured: value,
		PreviousDataDir: currentDir, SettingsPath: SettingsFile(),
		Copied: []string{}, Skipped: []string{}, RestartRequired: true,
		Hint: "restart the service (kp restart) to use the new data directory",
	}
	if filepath.Clean(resolved) == filepath.Clean(currentDir) {
		res.Status = "unchanged"
		res.Hint = "this is already the active data directory"
	} else if copyCurrent {
		cp, err := CopyDataDir(currentDir, resolved)
		if err != nil {
			return nil, err
		}
		res.Copied, res.Skipped = cp.Copied, cp.Skipped
	}
	if res.Copied == nil {
		res.Copied = []string{}
	}
	if res.Skipped == nil {
		res.Skipped = []string{}
	}

	// Keep the raw (possibly relative) value: it stays portable when the whole
	// kp folder is moved. Migration markers are preserved by LoadSettings.
	s.DataDir = value
	if err := SaveSettings(s); err != nil {
		return nil, err
	}
	return res, nil
}

// copyLayout copies config.yaml and the audit.db group from src to dest.
// The SQLite group is all-or-nothing: if dest/audit.db already exists, none of
// the -wal/-shm files are copied (mismatched WAL would corrupt the db).
func copyLayout(src, dest string) (copied, skipped []string, err error) {
	// Always return [] instead of null so CLI/REST JSON stays stable.
	copied, skipped = []string{}, []string{}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create data dir %s: %w", dest, err)
	}

	// 1) config.yaml (standalone text file)
	fromCfg := filepath.Join(src, configFileName)
	toCfg := filepath.Join(dest, configFileName)
	if fileExists(toCfg) {
		skipped = append(skipped, configFileName)
	} else if fileExists(fromCfg) {
		if err := copyFile(fromCfg, toCfg); err != nil {
			return nil, nil, fmt.Errorf("copy %s: %w", configFileName, err)
		}
		copied = append(copied, configFileName)
	}

	// 2) audit.db + WAL companions as one group
	toDB := filepath.Join(dest, auditDBName)
	fromDB := filepath.Join(src, auditDBName)
	switch {
	case fileExists(toDB):
		skipped = append(skipped, "audit.db*")
	case !fileExists(fromDB):
		// nothing to move
	default:
		for _, f := range auditDBGroup {
			from := filepath.Join(src, f)
			if !fileExists(from) {
				continue
			}
			to := filepath.Join(dest, f)
			if fileExists(to) {
				// orphan WAL/SHM in an otherwise empty dest: leave it alone.
				skipped = append(skipped, f)
				continue
			}
			if err := copyFile(from, to); err != nil {
				return nil, nil, fmt.Errorf("copy %s: %w", f, err)
			}
			copied = append(copied, f)
		}
	}
	return copied, skipped, nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// copyFile copies a single file (dest must not already exist).
func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}
