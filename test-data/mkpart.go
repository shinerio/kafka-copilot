// Test helper: create a topic with many partitions so the partition
// multi-select dropdown can be exercised with real data.
// Usage: go run ./test-data/mkpart.go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("192.168.85.2:9092"),
		kgo.ClientID("kp-test-setup"),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		panic(fmt.Errorf("ping: %w", err))
	}

	req := kmsg.NewCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{
		{Topic: "kp-multi-part", NumPartitions: 12, ReplicationFactor: 1},
	}
	req.TimeoutMillis = 10000
	resp, err := cl.Request(ctx, &req)
	if err != nil {
		panic(fmt.Errorf("create topics: %w", err))
	}
	cr := resp.(*kmsg.CreateTopicsResponse)
	for _, t := range cr.Topics {
		msg := ""
		if t.ErrorMessage != nil {
			msg = *t.ErrorMessage
		}
		fmt.Printf("topic=%s code=%d msg=%s\n", t.Topic, t.ErrorCode, msg)
	}
}
