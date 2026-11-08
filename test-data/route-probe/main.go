// Test helper: prove that "the topic was never resized" is NOT sufficient for
// narrowing a scan by key hash.
//
// kp-multi-part has 12 partitions and has NEVER been resized. For the key
// "kp-routing-probe", murmur2 -> P0. This helper writes the SAME key twice:
//
//	P0 -- where a standard Java / franz-go producer would put it
//	P1 -- where a producer with an explicit or custom partitioner put it
//
// A full scan finds 2 records; a key-located scan (P0 only) finds 1 and
// silently misses the other, even though the partition count never changed.
//
// Usage: go run ./test-data/route-probe
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("192.168.85.2:9092"),
		kgo.ClientID("kp-routing-probe"),
		// ManualPartitioner honours Record.Partition verbatim, standing in for
		// any producer that pins a partition or uses a non-standard hasher.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		panic(fmt.Errorf("ping: %w", err))
	}

	const key = "kp-routing-probe"
	for _, p := range []int32{0, 1} {
		r := &kgo.Record{
			Topic:     "kp-multi-part",
			Partition: p,
			Key:       []byte(key),
			Value:     []byte(fmt.Sprintf(`{"probe":"manual-partition","partition":%d}`, p)),
		}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			panic(fmt.Errorf("produce to P%d: %w", p, err))
		}
		fmt.Printf("wrote key=%s -> P%d\n", key, p)
	}
	fmt.Println("murmur2(key) picks P0 on this 12-partition topic")
}
