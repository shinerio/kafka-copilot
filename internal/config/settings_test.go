package config

import (
	"os"
	"path/filepath"
	"testing"
)

// isolate points settings + legacy app-data into a temp tree so tests never
// touch the real kp-settings.yaml or the user's %APPDATA%/kafka-pilot.
func isolate(t *testing.T) (tmp, legacy, data string) {
	t.Helper()
	tmp = t.TempDir()
	t.Setenv("KP_SETTINGS", filepath.Join(tmp, "kp-settings.yaml"))
	t.Setenv("KP_APPDATA", "")
	t.Setenv("APPDATA", filepath.Join(tmp, "appdata"))
	legacy = LegacyAppDataDir()
	data = filepath.Join(tmp, "kp-data")
	return tmp, legacy, data
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestResolveDataDir(t *testing.T) {
	if got := ResolveDataDir(""); got != DefaultDataDir() {
		t.Fatalf("empty -> default: got %q want %q", got, DefaultDataDir())
	}
	if got := ResolveDataDir("."); got != DefaultDataDir() {
		t.Fatalf("dot -> default: got %q", got)
	}
	abs := filepath.Join(t.TempDir(), "custom")
	if got := ResolveDataDir(abs); got != abs {
		t.Fatalf("absolute kept: got %q want %q", got, abs)
	}
	want := filepath.Join(ExeDir(), "my-data")
	if got := ResolveDataDir("my-data"); got != want {
		t.Fatalf("relative resolves against exe dir: got %q want %q", got, want)
	}
}

func TestSettingsRoundTripAndDefaults(t *testing.T) {
	isolate(t)
	s, err := LoadSettings()
	if err != nil {
		t.Fatal(err)
	}
	if s.DataDir != DefaultDataDir() {
		t.Fatalf("first load should persist the default, got %q", s.DataDir)
	}
	if !fileExists(SettingsFile()) {
		t.Fatal("LoadSettings should materialize kp-settings.yaml")
	}

	s.DataDir = `D:/data/kp`
	s.MigrationDone = true
	s.MigratedFrom = `C:/legacy`
	s.MigratedAt = "2026-11-08T22:00:00+08:00"
	if err := SaveSettings(s); err != nil {
		t.Fatal(err)
	}
	again, err := LoadSettings()
	if err != nil {
		t.Fatal(err)
	}
	if again.DataDir != s.DataDir || !again.MigrationDone ||
		again.MigratedFrom != s.MigratedFrom || again.MigratedAt != s.MigratedAt {
		t.Fatalf("settings did not round-trip: %+v", again)
	}
}

func TestCopyLayoutCopiesSQLiteGroupTogether(t *testing.T) {
	tmp := t.TempDir()
	src, dest := filepath.Join(tmp, "src"), filepath.Join(tmp, "dest")
	writeFile(t, filepath.Join(src, "config.yaml"), "default_profile: test\n")
	writeFile(t, filepath.Join(src, "audit.db"), "db")
	writeFile(t, filepath.Join(src, "audit.db-wal"), "wal")
	writeFile(t, filepath.Join(src, "audit.db-shm"), "shm")

	copied, skipped, err := copyLayout(src, dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(copied) != 4 {
		t.Fatalf("expected 4 copied files, got %v", copied)
	}
	if len(skipped) != 0 {
		t.Fatalf("expected no skipped files, got %v", skipped)
	}
	for _, f := range append([]string{"config.yaml"}, auditDBGroup...) {
		if !fileExists(filepath.Join(dest, f)) {
			t.Fatalf("missing %s in destination", f)
		}
	}
	// Source is intentionally kept: migration is non-destructive / rollbackable.
	if !fileExists(filepath.Join(src, "config.yaml")) {
		t.Fatal("source file must be kept")
	}
}

func TestCopyLayoutNeverMixesWALIntoExistingDB(t *testing.T) {
	tmp := t.TempDir()
	src, dest := filepath.Join(tmp, "src"), filepath.Join(tmp, "dest")
	writeFile(t, filepath.Join(src, "config.yaml"), "src config")
	writeFile(t, filepath.Join(src, "audit.db"), "src db")
	writeFile(t, filepath.Join(src, "audit.db-wal"), "src wal")
	writeFile(t, filepath.Join(src, "audit.db-shm"), "src shm")
	// A database already exists in dest. Copying the src WAL here would corrupt
	// it, so the whole SQLite group must be skipped.
	writeFile(t, filepath.Join(dest, "audit.db"), "dest db")
	writeFile(t, filepath.Join(dest, "config.yaml"), "dest config")

	copied, skipped, err := copyLayout(src, dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(copied) != 0 {
		t.Fatalf("nothing should be copied, got %v", copied)
	}
	if len(skipped) != 2 {
		t.Fatalf("expected config.yaml + audit.db* skipped, got %v", skipped)
	}
	for _, f := range []string{"audit.db-wal", "audit.db-shm"} {
		if fileExists(filepath.Join(dest, f)) {
			t.Fatalf("%s must NOT be copied next to an existing db", f)
		}
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "audit.db")); string(got) != "dest db" {
		t.Fatal("existing db was overwritten")
	}
}

func TestMigrateDataIsOneTimeAndForceIsSafe(t *testing.T) {
	_, legacy, data := isolate(t)
	writeFile(t, filepath.Join(legacy, "config.yaml"), "default_profile: prod\n")
	writeFile(t, filepath.Join(legacy, "audit.db"), "db")
	writeFile(t, filepath.Join(legacy, "audit.db-wal"), "wal")

	res, err := MigrateData(data, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.AlreadyDone || len(res.Copied) != 3 {
		t.Fatalf("unexpected first migration result: %+v", res)
	}
	if s, _ := LoadSettings(); !s.MigrationDone || s.MigratedFrom != legacy {
		t.Fatalf("migration marker not persisted: %+v", s)
	}

	again, err := MigrateData(data, false)
	if err != nil {
		t.Fatal(err)
	}
	if !again.AlreadyDone {
		t.Fatal("second automatic migration must be a no-op")
	}

	forced, err := MigrateData(data, true)
	if err != nil {
		t.Fatal(err)
	}
	if forced.AlreadyDone || len(forced.Copied) != 0 {
		t.Fatalf("forced re-check must never overwrite existing files: %+v", forced)
	}
}

func TestMigrateDataSkipsWhenEnvOverrideEqualsSource(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("KP_SETTINGS", filepath.Join(tmp, "kp-settings.yaml"))
	override := filepath.Join(tmp, "override")
	t.Setenv("KP_APPDATA", override)
	res, err := MigrateData(override, false)
	if err != nil {
		t.Fatal(err)
	}
	if !res.AlreadyDone {
		t.Fatal("KP_APPDATA source == dest must be a no-op")
	}
}

func TestCopyDataDir(t *testing.T) {
	tmp := t.TempDir()
	from, to := filepath.Join(tmp, "old"), filepath.Join(tmp, "new")
	writeFile(t, filepath.Join(from, "config.yaml"), "cfg")
	writeFile(t, filepath.Join(from, "audit.db"), "db")

	res, err := CopyDataDir(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Copied) != 2 || !res.RestartRequired {
		t.Fatalf("unexpected result: %+v", res)
	}
	if !fileExists(filepath.Join(to, "config.yaml")) || !fileExists(filepath.Join(to, "audit.db")) {
		t.Fatal("data was not carried over to the new directory")
	}
	same, err := CopyDataDir(from, from)
	if err != nil {
		t.Fatal(err)
	}
	if !same.AlreadyDone {
		t.Fatal("same-directory copy must be a no-op")
	}
}

func TestSetDataDirCopiesCurrentAndKeepsMarkers(t *testing.T) {
	tmp, _, _ := isolate(t)
	oldDir := filepath.Join(tmp, "old")
	newDir := filepath.Join(tmp, "new")
	writeFile(t, filepath.Join(oldDir, "config.yaml"), "cfg")
	writeFile(t, filepath.Join(oldDir, "audit.db"), "db")
	writeFile(t, filepath.Join(oldDir, "audit.db-wal"), "wal")

	s, err := LoadSettings()
	if err != nil {
		t.Fatal(err)
	}
	s.DataDir = oldDir
	s.MigrationDone = true
	s.MigratedFrom = "legacy"
	if err := SaveSettings(s); err != nil {
		t.Fatal(err)
	}

	res, err := SetDataDir(newDir, oldDir, true)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "saved" || len(res.Copied) != 3 || !res.RestartRequired {
		t.Fatalf("unexpected update result: %+v", res)
	}
	if !fileExists(filepath.Join(newDir, "config.yaml")) || !fileExists(filepath.Join(newDir, "audit.db")) {
		t.Fatal("data was not carried into the new directory")
	}
	after, err := LoadSettings()
	if err != nil {
		t.Fatal(err)
	}
	if after.DataDir != newDir || !after.MigrationDone || after.MigratedFrom != "legacy" {
		t.Fatalf("settings markers were lost: %+v", after)
	}

	third := filepath.Join(tmp, "third")
	res2, err := SetDataDir(third, newDir, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Copied) != 0 || fileExists(filepath.Join(third, "config.yaml")) {
		t.Fatalf("copy_current=false must not copy anything: %+v", res2)
	}
}

func TestDataLayout(t *testing.T) {
	_, legacy, data := isolate(t)
	writeFile(t, filepath.Join(legacy, "config.yaml"), "legacy")
	l, err := DataLayout()
	if err != nil {
		t.Fatal(err)
	}
	if l.LegacyDir != legacy || l.SettingsPath != SettingsFile() {
		t.Fatalf("unexpected layout: %+v", l)
	}
	if !l.LegacyConfigExists || l.AuditExists || l.ConfigExists {
		t.Fatalf("wrong existence flags: %+v", l)
	}
	if l.DataDir != DefaultDataDir() || l.DefaultDataDir != DefaultDataDir() {
		t.Fatalf("default data dir mismatch: %+v", l)
	}
	_ = data
}
