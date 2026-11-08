package scan

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"kafka-pilot/internal/filter"
)

// MaxLast is the hard cap for a single backscan. The Web client exposes
// exactly five radio options (100/500/1000/5000/10000); the CLI still accepts
// any value up to this cap.
const MaxLast = 10000

// MaxResultMessages is a safety cap on the total number of hits returned by a
// single scan. --last N is a PER-PARTITION backscan amount, so a topic with P
// partitions can legitimately produce P*N hits; all of them are returned so
// no partition is silently dropped. This cap only guards pathological cases
// (e.g. 100+ partitions x 10000) from exhausting memory / the HTTP response.
const MaxResultMessages = 50000

// Request holds scan parameters.
type Request struct {
	Topic          string
	Partitions     []int32 // nil = all partitions
	Last           int     // backscan count (回扫量语义)
	Since          time.Time
	Until          time.Time
	HasTimeWindow  bool
	OffsetFrom     int64
	OffsetTo       int64
	HasOffsetRange bool
	Timeout        time.Duration
}

// PartitionRange records the actual scan range for a partition.
type PartitionRange struct {
	Partition int32 `json:"partition"`
	From      int64 `json:"from_offset"`
	To        int64 `json:"to_offset"`
}

// Result holds scan output.
type Result struct {
	OpID              string           `json:"opId"`
	Topic             string           `json:"topic"`
	Total             int              `json:"total"`
	Matched           int              `json:"matched"`
	// MatchedRaw is the number of filter hits before a safety-cap truncation.
	// Only emitted when a truncation actually happened, so the UI can state
	// exactly how many hits were dropped instead of showing a bare warning code.
	MatchedRaw        int              `json:"matched_raw,omitempty"`
	Truncated         bool             `json:"truncated"`
	Warnings          []string         `json:"warnings,omitempty"`
	Messages          []*filter.Result `json:"messages"`
	PartitionRanges   []PartitionRange `json:"partition_ranges,omitempty"`
	ElapsedMs         int64            `json:"elapsedMs"`
	SkippedNonUTF8Key int              `json:"skippedNonUtf8Key,omitempty"`
}

// Scanner performs partition-parallel scan with direct assign.
type Scanner struct {
	client *kgo.Client
}

// New creates a scanner from a kgo.Client.
func New(client *kgo.Client) *Scanner {
	return &Scanner{client: client}
}

// partitionScanRange describes the per-partition scan window.
type partitionScanRange struct {
	partition   int32
	startOffset int64
	endOffset   int64
}

// partState tracks per-partition progress during polling.
type partState struct {
	start    int64
	end      int64
	hwm      int64
	consumed int
	maxOff   int64
	done     bool
}

// Scan executes the scan request with three-layer filtering.
//
// Pipeline:
//  1. Resolve target partitions (use kmsg metadata if req.Partitions empty).
//  2. Compute [start, end) offset window per partition via kmsg.ListOffsets.
//  3. Direct-assign partitions with kgo.AddConsumePartitions (no group, no commits).
//  4. PollFetches loop, applying offset/time windows and the filter.
//  5. Cross-partition merge-sort by timestamp desc (tie-break: partition asc, offset asc).
//  6. Return all hits (per-partition backscan, no global --last truncation),
//     guarded only by a very high safety cap; flag timeout/cap truncation.
func (s *Scanner) Scan(ctx context.Context, req *Request, f *filter.Filter) (*Result, error) {
	start := time.Now()

	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	// 1. Resolve partition list.
	partIDs, err := s.resolvePartitions(ctx, req)
	if err != nil {
		return nil, err
	}

	// 2. Calculate scan ranges per partition.
	ranges, err := s.calculateRanges(ctx, req, partIDs)
	if err != nil {
		return nil, err
	}

	// Fast path: no partition has any message in the requested window (a freshly
	// created empty topic, or a window entirely past the HWM). Entering
	// PollFetches here would block on the broker's long poll until the scan
	// timeout for no reason, so return the empty result immediately.
	hasData := false
	for _, r := range ranges {
		if r.endOffset > r.startOffset {
			hasData = true
			break
		}
	}
	if !hasData {
		pr := make([]PartitionRange, 0, len(ranges))
		for _, r := range ranges {
			pr = append(pr, PartitionRange{
				Partition: r.partition,
				From:      r.startOffset,
				To:        r.endOffset,
			})
		}
		sort.Slice(pr, func(i, j int) bool { return pr[i].Partition < pr[j].Partition })
		return &Result{
			Topic:           req.Topic,
			Messages:        []*filter.Result{},
			PartitionRanges: pr,
			ElapsedMs:       time.Since(start).Milliseconds(),
		}, nil
	}

	// 3. Direct-assign partitions at their start offsets.
	assignMap := map[string]map[int32]kgo.Offset{req.Topic: {}}
	for _, r := range ranges {
		assignMap[req.Topic][r.partition] = kgo.NewOffset().At(r.startOffset)
	}
	s.client.AddConsumePartitions(assignMap)
	defer s.client.RemoveConsumePartitions(map[string][]int32{req.Topic: partIDs})

	// 4. PollFetches loop with per-partition completion tracking.
	states := make(map[int32]*partState, len(ranges))
	for _, r := range ranges {
		states[r.partition] = &partState{
			start: r.startOffset,
			end:   r.endOffset,
			hwm:   r.endOffset, // refined below from calculateRanges
		}
	}

	var (
		mu            sync.Mutex
		allMessages   []*filter.Result
		totalScanned  int
		partitionErrs []string
	)

pollLoop:
	for {
		if ctx.Err() != nil {
			break
		}
		fetches := s.client.PollFetches(ctx)

		// Surface fetch-level errors (e.g. client closed). Context-cancellation
		// errors are expected during timeout and are silently absorbed.
		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				break pollLoop
			}
			partitionErrs = append(partitionErrs, err.Error())
			break pollLoop
		}

		if fetches.Empty() {
			if allPartitionsDone(states) {
				break pollLoop
			}
			continue
		}

		fetches.EachPartition(func(tp kgo.FetchTopicPartition) {
			if tp.Topic != req.Topic {
				return
			}
			st := states[tp.Partition]
			if st == nil || st.done {
				return
			}
			if tp.Err != nil {
				partitionErrs = append(partitionErrs,
					fmt.Sprintf("partition %d: %s", tp.Partition, tp.Err))
				st.done = true
				return
			}

			for _, r := range tp.Records {
				if r.Offset < st.start {
					continue
				}
				if r.Offset >= st.end {
					st.done = true
					break
				}
				// Time-window filtering (for --since/--until mode where end == HWM).
				if req.HasTimeWindow {
					if !req.Since.IsZero() && r.Timestamp.Before(req.Since) {
						continue
					}
					if !req.Until.IsZero() && r.Timestamp.After(req.Until) {
						st.done = true
						break
					}
				}

				totalScanned++
				st.consumed++
				st.maxOff = r.Offset

				msg := &filter.Message{
					Partition: r.Partition,
					Offset:    r.Offset,
					Timestamp: r.Timestamp,
					Key:       r.Key,
					Value:     r.Value,
					Headers:   extractHeaders(r),
					Size:      len(r.Key) + len(r.Value),
				}
				if result, matched := f.Apply(msg); matched {
					mu.Lock()
					allMessages = append(allMessages, result)
					mu.Unlock()
				}

				// --last N backscan: per-partition cap.
				if req.Last > 0 && !req.HasOffsetRange && !req.HasTimeWindow && st.consumed >= req.Last {
					st.done = true
					break
				}
			}

			// Mark done if we have read past the end offset or reached HWM.
			if !st.done && tp.HighWatermark <= st.end {
				st.done = true
			}
		})

		if allPartitionsDone(states) {
			break pollLoop
		}
	}

	// 5. Merge-sort across partitions by timestamp descending.
	sort.Slice(allMessages, func(i, j int) bool {
		ti := allMessages[i].Timestamp
		tj := allMessages[j].Timestamp
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		if allMessages[i].Partition != allMessages[j].Partition {
			return allMessages[i].Partition < allMessages[j].Partition
		}
		return allMessages[i].Offset < allMessages[j].Offset
	})

	// 6. Result assembly. --last N is a PER-PARTITION backscan amount: the poll
	// loop already reads at most N raw messages from each partition, so each
	// partition contributes at most N hits. Do NOT globally truncate to N --
	// that used to discard whole partitions and produced confusing results such
	// as "total 1200, matched 100" on a 12-partition topic. Only a very high
	// safety cap (and timeout below) may truncate.
	truncated := false
	matchedRaw := 0
	warnings := []string{}
	// FR-05: large backscans are allowed but always flagged in the result.
	if req.Last > 1000 {
		warnings = append(warnings, "scan>1000")
	}
	if len(allMessages) > MaxResultMessages {
		matchedRaw = len(allMessages)
		// Truncate PER PARTITION, not globally. A global time-ordered cut can
		// erase entire partitions -- exactly the data the user must be able to
		// inspect. allMessages is already sorted by ts desc, so taking the
		// first K hits per partition keeps each partition's newest results.
		perPart := MaxResultMessages / len(partIDs)
		if perPart < 1 {
			perPart = 1
		}
		kept := make([]*filter.Result, 0, MaxResultMessages)
		perPartCount := make(map[int32]int, len(partIDs))
		for _, m := range allMessages {
			if perPartCount[m.Partition] >= perPart {
				continue
			}
			perPartCount[m.Partition]++
			kept = append(kept, m)
		}
		allMessages = kept
		truncated = true
		warnings = append(warnings, "result-cap")
	}
	if ctx.Err() == context.DeadlineExceeded {
		truncated = true
		warnings = append(warnings, "timeout")
	}
	if len(partitionErrs) > 0 {
		warnings = append(warnings, partitionErrs...)
	}

	// 7. Build ordered partition range summary.
	partitionRanges := make([]PartitionRange, 0, len(ranges))
	for _, r := range ranges {
		partitionRanges = append(partitionRanges, PartitionRange{
			Partition: r.partition,
			From:      r.startOffset,
			To:        r.endOffset,
		})
	}
	sort.Slice(partitionRanges, func(i, j int) bool {
		return partitionRanges[i].Partition < partitionRanges[j].Partition
	})

	return &Result{
		Topic:             req.Topic,
		Total:             totalScanned,
		Matched:           len(allMessages),
		MatchedRaw:        matchedRaw,
		Truncated:         truncated,
		Warnings:          warnings,
		Messages:          allMessages,
		PartitionRanges:   partitionRanges,
		ElapsedMs:         time.Since(start).Milliseconds(),
		SkippedNonUTF8Key: f.SkippedNonUTF8Key,
	}, nil
}

// allPartitionsDone reports whether every tracked partition has finished.
func allPartitionsDone(states map[int32]*partState) bool {
	for _, st := range states {
		if !st.done {
			return false
		}
	}
	return true
}

// resolvePartitions returns the partition list to scan. If req.Partitions is
// non-empty it is returned verbatim; otherwise all partitions for the topic
// are discovered via a kmsg.MetadataRequest.
func (s *Scanner) resolvePartitions(ctx context.Context, req *Request) ([]int32, error) {
	if len(req.Partitions) > 0 {
		return req.Partitions, nil
	}
	topicMeta, err := s.fetchTopicMetadata(ctx, req.Topic)
	if err != nil {
		return nil, err
	}
	parts := make([]int32, 0, len(topicMeta.Partitions))
	for _, p := range topicMeta.Partitions {
		parts = append(parts, p.Partition)
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("topic %s has no partitions", req.Topic)
	}
	return parts, nil
}

// calculateRanges computes the [start, end) offset window per partition using
// the precedence: explicit offset range > time window > --last N backscan.
func (s *Scanner) calculateRanges(ctx context.Context, req *Request, partIDs []int32) ([]partitionScanRange, error) {
	// Always need HWM (latest offset) for end bound / backscan base.
	hwmMap, err := s.fetchOffsets(ctx, req.Topic, partIDs, -1) // -1 = latest
	if err != nil {
		return nil, fmt.Errorf("list latest offsets: %w", err)
	}

	// For time-window mode, fetch the start offset matching req.Since.
	var sinceOffsets map[int32]int64
	if req.HasTimeWindow && !req.Since.IsZero() {
		sinceOffsets, err = s.fetchOffsets(ctx, req.Topic, partIDs, req.Since.UnixMilli())
		if err != nil {
			return nil, fmt.Errorf("list offsets for since: %w", err)
		}
	}

	ranges := make([]partitionScanRange, 0, len(partIDs))
	for _, p := range partIDs {
		hwm := hwmMap[p]
		var start, end int64

		switch {
		case req.HasOffsetRange:
			start = req.OffsetFrom
			end = req.OffsetTo
			if end <= 0 || end > hwm {
				end = hwm
			}
		case req.HasTimeWindow:
			if so, ok := sinceOffsets[p]; ok && so > 0 {
				start = so
			} else {
				start = 0
			}
			// End stays at HWM; timestamp filter inside the poll loop enforces Until.
			end = hwm
		default: // --last N
			start = hwm - int64(req.Last)
			if start < 0 {
				start = 0
			}
			end = hwm
		}

		// If the partition is empty (HWM == 0) or start is past HWM, skip scanning
		// by emitting a zero-width range that the poll loop will immediately mark done.
		if start < 0 {
			start = 0
		}
		if end < start {
			end = start
		}
		ranges = append(ranges, partitionScanRange{
			partition:   p,
			startOffset: start,
			endOffset:   end,
		})
	}
	return ranges, nil
}

// fetchTopicMetadata issues a kmsg.MetadataRequest for a single topic and
// returns the matching response topic entry.
func (s *Scanner) fetchTopicMetadata(ctx context.Context, topic string) (*kmsg.MetadataResponseTopic, error) {
	req := kmsg.NewMetadataRequest()
	topicName := topic
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: &topicName}}

	resp, err := s.client.Request(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("metadata for topic %s: %w", topic, err)
	}
	metaResp, ok := resp.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected metadata response type %T", resp)
	}
	for i := range metaResp.Topics {
		t := &metaResp.Topics[i]
		if t.Topic != nil && *t.Topic == topic {
			if t.ErrorCode != 0 {
				return nil, fmt.Errorf("metadata for topic %s: %w", topic, kerr.ErrorForCode(t.ErrorCode))
			}
			return t, nil
		}
	}
	return nil, fmt.Errorf("topic %s not found", topic)
}

// fetchOffsets issues a kmsg.ListOffsetsRequest for the given partitions at
// the given timestamp (-1 latest, -2 earliest, or a UnixMilli). Returns a
// partition→offset map.
func (s *Scanner) fetchOffsets(ctx context.Context, topic string, partitions []int32, timestamp int64) (map[int32]int64, error) {
	req := kmsg.NewListOffsetsRequest()
	req.ReplicaID = -1

	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = topic
	for _, p := range partitions {
		partReq := kmsg.NewListOffsetsRequestTopicPartition()
		partReq.Partition = p
		partReq.Timestamp = timestamp
		topicReq.Partitions = append(topicReq.Partitions, partReq)
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{topicReq}

	resp, err := s.client.Request(ctx, &req)
	if err != nil {
		return nil, err
	}
	offsetResp, ok := resp.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected list offsets response type %T", resp)
	}

	result := make(map[int32]int64, len(partitions))
	for _, t := range offsetResp.Topics {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				continue
			}
			result[p.Partition] = p.Offset
		}
	}
	return result, nil
}

// extractHeaders converts a record's headers to the filter.Message header map.
func extractHeaders(r *kgo.Record) map[string][]byte {
	if len(r.Headers) == 0 {
		return nil
	}
	headers := make(map[string][]byte, len(r.Headers))
	for _, h := range r.Headers {
		headers[h.Key] = h.Value
	}
	return headers
}

// ValidateRequest checks scan parameters and returns error if invalid.
func ValidateRequest(req *Request) error {
	if req.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if req.Last > MaxLast {
		return fmt.Errorf("--last %d exceeds maximum %d", req.Last, MaxLast)
	}
	if req.Last <= 0 && !req.HasTimeWindow && !req.HasOffsetRange {
		return fmt.Errorf("at least one of --last, --since/--until, --offset-from/--offset-to is required")
	}
	if req.HasOffsetRange && req.HasTimeWindow {
		return fmt.Errorf("--offset-from/--offset-to and --since/--until are mutually exclusive")
	}
	if req.HasOffsetRange && req.Last > 0 && !req.HasTimeWindow {
		return fmt.Errorf("--offset-from/--offset-to and --last are mutually exclusive")
	}
	return nil
}
