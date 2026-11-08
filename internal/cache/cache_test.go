package cache

import (
	"testing"
	"time"
)

func mkEntries(start, n int64) []Entry {
	out := make([]Entry, 0, n)
	for i := int64(0); i < n; i++ {
		off := start + i
		out = append(out, Entry{
			Partition: 0,
			Offset:    off,
			Timestamp: time.UnixMilli(off),
			Key:       []byte("k"),
			Value:     []byte("v"),
			Headers:   map[string][]byte{"h": []byte("1")},
		})
	}
	return out
}

func openTest(t *testing.T, cfg Config) *Store {
	t.Helper()
	s, err := Open(t.TempDir()+"/cache.db", cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAppendAndGetRange(t *testing.T) {
	s := openTest(t, Config{MaxEntries: 0, MaxBytes: 0})
	if err := s.Append("p", "t", "T1", 0, mkEntries(0, 10)); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRange("p", "t", 0, 3, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("want 4 entries [3,7), got %d", len(got))
	}
	for i, e := range got {
		if e.Offset != int64(3+i) {
			t.Fatalf("entry %d offset = %d, want %d", i, e.Offset, 3+i)
		}
	}
}

func TestEntryCapEvictsOldest(t *testing.T) {
	// Cap at 5 messages; append 10 (offsets 0..9). After enforce, only the
	// 5 most-recent offsets (5..9) must survive.
	s := openTest(t, Config{MaxEntries: 5, MaxBytes: 0})
	if err := s.Append("p", "t", "T1", 0, mkEntries(0, 10)); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.TotalEntries != 5 {
		t.Fatalf("total entries = %d, want 5", st.TotalEntries)
	}
	got, _ := s.GetRange("p", "t", 0, 0, 100)
	if len(got) != 5 {
		t.Fatalf("survivors = %d, want 5", len(got))
	}
	for i, e := range got {
		if e.Offset != int64(5+i) {
			t.Fatalf("survivor %d offset = %d, want %d (oldest must be dropped)", i, e.Offset, 5+i)
		}
	}
}

func TestByteCapEvictsOldest(t *testing.T) {
	// Each entry is len(k)+len(v)+len(h)+len("1") = 1+1+1+1 = 4 bytes logical.
	// Budget of 8 bytes => only 2 most-recent entries survive.
	s := openTest(t, Config{MaxEntries: 0, MaxBytes: 8})
	if err := s.Append("p", "t", "T1", 0, mkEntries(0, 5)); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.TotalBytes > 8 {
		t.Fatalf("total bytes = %d, want <= 8", st.TotalBytes)
	}
	if st.TotalEntries != 2 {
		t.Fatalf("entries = %d, want 2", st.TotalEntries)
	}
	got, _ := s.GetRange("p", "t", 0, 0, 100)
	if got[0].Offset != 3 || got[1].Offset != 4 {
		t.Fatalf("survivors = %v, want offsets [3,4]", []int64{got[0].Offset, got[1].Offset})
	}
}

func TestEvictBelow(t *testing.T) {
	s := openTest(t, Config{MaxEntries: 0, MaxBytes: 0})
	if err := s.Append("p", "t", "T1", 0, mkEntries(0, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.EvictBelow("p", "t", 0, 5); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.TotalEntries != 5 {
		t.Fatalf("after evict-below total = %d, want 5", st.TotalEntries)
	}
	got, _ := s.GetRange("p", "t", 0, 0, 100)
	for _, e := range got {
		if e.Offset < 5 {
			t.Fatalf("evict-below leaked offset %d", e.Offset)
		}
	}
}

func TestRecreationGuard(t *testing.T) {
	s := openTest(t, Config{MaxEntries: 0, MaxBytes: 0})
	if err := s.Append("p", "t", "T1", 0, mkEntries(0, 5)); err != nil {
		t.Fatal(err)
	}
	// Same topic name, different topicID => treated as recreated; old data purged.
	if err := s.Append("p", "t", "T2", 0, mkEntries(100, 3)); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRange("p", "t", 0, 0, 1000)
	if len(got) != 3 {
		t.Fatalf("after recreation got %d entries, want 3", len(got))
	}
	for _, e := range got {
		if e.Offset < 100 {
			t.Fatalf("stale offset %d survived recreation", e.Offset)
		}
	}
}

func TestClear(t *testing.T) {
	s := openTest(t, Config{MaxEntries: 0, MaxBytes: 0})
	_ = s.Append("p", "t", "T1", 0, mkEntries(0, 10))
	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.TotalEntries != 0 || st.TotalBytes != 0 {
		t.Fatalf("after clear stats = %+v, want empty", st)
	}
}

func TestSetLimitsTrimsLive(t *testing.T) {
	s := openTest(t, Config{MaxEntries: 0, MaxBytes: 0})
	_ = s.Append("p", "t", "T1", 0, mkEntries(0, 20))
	if err := s.SetLimits(Config{MaxEntries: 7, MaxBytes: 0}); err != nil {
		t.Fatal(err)
	}
	if s.Stats().TotalEntries != 7 {
		t.Fatalf("after SetLimits total = %d, want 7", s.Stats().TotalEntries)
	}
}
