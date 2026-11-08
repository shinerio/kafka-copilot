package produce

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SingleRequest holds single-message produce parameters.
// The target partition is never specified: franz-go derives it from the key.
type SingleRequest struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []kgo.RecordHeader
}

// Producer wraps a kgo.Client for producing.
type Producer struct {
	client *kgo.Client
}

// New creates a producer.
func New(client *kgo.Client) *Producer {
	return &Producer{client: client}
}

// ProduceSingle sends a single message (FR-06).
func (p *Producer) ProduceSingle(ctx context.Context, req *SingleRequest) (int32, int64, error) {
	rec := &kgo.Record{
		Topic:   req.Topic,
		Key:     req.Key,
		Value:   req.Value,
		Headers: req.Headers,
	}

	// Do not set Record.Partition: the client's default partitioner hashes the
	// key to choose a partition, which is the product-defined behavior.
	results := p.client.ProduceSync(ctx, rec)
	if err := results.FirstErr(); err != nil {
		return 0, 0, fmt.Errorf("produce: %w", err)
	}

	return rec.Partition, rec.Offset, nil
}

// ParseHeaders parses --header k=v arguments.
func ParseHeaders(headers []string) ([]kgo.RecordHeader, error) {
	var result []kgo.RecordHeader
	for _, h := range headers {
		parts := strings.SplitN(h, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header format %q, expected key=value", h)
		}
		result = append(result, kgo.RecordHeader{
			Key:   parts[0],
			Value: []byte(parts[1]),
		})
	}
	return result, nil
}

// ReadFile reads a file's content as bytes.
func ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
