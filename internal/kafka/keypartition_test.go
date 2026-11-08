package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Known vectors from an independent Python reimplementation of Kafka's
// murmur2, cross-checked against the real 12-partition kp-multi-part topic
// (order-9999 -> P4, order-9998 -> P8, order-9997 -> P11).
func TestKeyPartitionKnownVectors(t *testing.T) {
	cases := []struct {
		key  string
		n    int
		want int
	}{
		{"order-9999", 12, 4},
		{"order-9998", 12, 8},
		{"order-9997", 12, 11},
		{"A1001", 12, 1},
		{"order-0", 12, 4},
		{"中文key", 12, 4},
		{"kp-routing-probe", 12, 0},
		{"", 12, 9},
	}
	for _, c := range cases {
		if got := KeyPartition([]byte(c.key), c.n); got != c.want {
			t.Errorf("KeyPartition(%q, %d) = %d, want %d", c.key, c.n, got, c.want)
		}
	}
}

func TestMurmur2KnownHashes(t *testing.T) {
	cases := []struct {
		key  string
		want uint32
	}{
		{"order-9999", 3193536324},
		{"order-9998", 3282033844},
		{"order-9997", 2960754331},
		{"A1001", 2398149033},
		{"", 275646681},
		{"中文key", 1499799316},
	}
	for _, c := range cases {
		if got := Murmur2([]byte(c.key)); got != c.want {
			t.Errorf("Murmur2(%q) = %d, want %d", c.key, got, c.want)
		}
	}
}

// The authoritative check: our hint must agree with the partitioner that
// kafka-pilot's own producer actually uses (franz-go's default hasher) for
// every key and every partition count, including non-power-of-two counts.
func TestKeyPartitionMatchesFranzGoDefault(t *testing.T) {
	p := kgo.StickyKeyPartitioner(nil).ForTopic("kp-test")
	keys := [][]byte{
		[]byte("order-9999"), []byte("A1001"), []byte(""),
		[]byte("中文key"), {0x00, 0xff, 0x10, 0x7a, 0x42},
		[]byte("a-very-long-key-with-binary-\x00-suffix-0123456789"),
	}
	for _, n := range []int{1, 2, 3, 7, 12, 32, 97} {
		for _, key := range keys {
			want := p.Partition(&kgo.Record{Key: key}, n)
			if got := KeyPartition(key, n); got != want {
				t.Errorf("KeyPartition(%q, %d) = %d, franz-go = %d", key, n, got, want)
			}
		}
	}
}

func TestKeyPartitionInvalidCount(t *testing.T) {
	for _, n := range []int{0, -1} {
		if got := KeyPartition([]byte("x"), n); got != 0 {
			t.Errorf("KeyPartition with n=%d = %d, want 0", n, got)
		}
	}
}
