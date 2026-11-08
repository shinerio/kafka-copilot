package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"kafka-pilot/internal/config"
)

// TopicInfo holds minimal topic metadata (FR-01: name, partitions, latest offset).
type TopicInfo struct {
	Name            string          `json:"name"`
	PartitionsCount int             `json:"partitions_count"`
	Partitions      []PartitionInfo `json:"partitions"`
}

// PartitionInfo holds per-partition info.
type PartitionInfo struct {
	Partition    int32 `json:"partition"`
	LatestOffset int64 `json:"latest_offset"`
}

// GroupInfo holds consumer group metadata.
type GroupInfo struct {
	Name      string         `json:"name"`
	State     string         `json:"state"`
	Consumers []ConsumerInfo `json:"consumers,omitempty"`
}

// ConsumerInfo holds a consumer group member.
type ConsumerInfo struct {
	ClientID    string   `json:"client_id"`
	MemberID    string   `json:"member_id"`
	ClientHost  string   `json:"client_host"`
	Assignments []string `json:"assignments"`
}

// LagInfo holds per-topic lag for a consumer group.
type LagInfo struct {
	Topic      string         `json:"topic"`
	Partitions []PartitionLag `json:"partitions"`
	TotalLag   int64          `json:"total_lag"`
	HighLag    bool           `json:"high_lag"`
}

// PartitionLag holds per-partition lag.
type PartitionLag struct {
	Partition int32 `json:"partition"`
	Committed int64 `json:"committed"`
	HWM       int64 `json:"hwm"`
	Lag       int64 `json:"lag"`
}

// Client wraps a kgo.Client with connection config.
type Client struct {
	kgoClient *kgo.Client
	profile   *config.Profile
}

// Pool manages Kafka clients per profile.
type Pool struct {
	mu      sync.Mutex
	clients map[string]*Client
}

// NewPool creates a new connection pool.
func NewPool() *Pool {
	return &Pool{clients: make(map[string]*Client)}
}

// Get returns or creates a client for the given profile.
func (p *Pool) Get(profile *config.Profile) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok := p.clients[profile.Name]; ok {
		return c, nil
	}

	c, err := newClient(profile)
	if err != nil {
		return nil, err
	}
	p.clients[profile.Name] = c
	return c, nil
}

// Invalidate drops the cached client for a profile so the next Get rebuilds
// it from fresh configuration. This matters whenever a profile's SASL/TLS
// settings change at runtime: without it, the connection pool keeps handing
// out the old (pre-change) client and the new credentials never take effect
// (a classic "I edited the profile but it still fails" trap). In-flight
// operations keep their own references; only future Gets are affected.
func (p *Pool) Invalidate(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[name]; ok {
		c.Close()
		delete(p.clients, name)
	}
}

// Close closes all clients in the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clients {
		c.Close()
	}
	p.clients = make(map[string]*Client)
}

// newClient creates a client with franz-go's native partitioning: keyed
// records are consistently hashed by key, keyless records are spread evenly.
// Producers never pin a partition explicitly (product decision).
func newClient(profile *config.Profile) (*Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(profile.BootstrapServers...),
		kgo.ClientID("kafka-pilot"),
	}

	if profile.SASL != nil && profile.SASL.Mechanism != "" {
		saslOpt, err := buildSASL(profile.SASL)
		if err != nil {
			return nil, fmt.Errorf("SASL config: %w", err)
		}
		opts = append(opts, saslOpt)
	}
	if profile.TLS != nil {
		tlsCfg, err := buildTLSConfig(profile.TLS)
		if err != nil {
			return nil, fmt.Errorf("TLS config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, classifyConnError(err, profile)
	}
	return &Client{kgoClient: cl, profile: profile}, nil
}

// buildSASL builds a kgo.SASL opt from the configured mechanism.
func buildSASL(s *config.SASL) (kgo.Opt, error) {
	user, pass := s.Username, s.Password
	switch strings.ToLower(s.Mechanism) {
	case "plain":
		return kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{User: user, Pass: pass}, nil
		})), nil
	case "scram-sha-256":
		return kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: user, Pass: pass}, nil
		})), nil
	case "scram-sha-512":
		return kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: user, Pass: pass}, nil
		})), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", s.Mechanism)
	}
}

func buildTLSConfig(tlsCfg *config.TLS) (*tls.Config, error) {
	cfg := &tls.Config{}

	if tlsCfg.CACert != "" {
		caData, err := os.ReadFile(tlsCfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		cfg.RootCAs = pool
	}

	if tlsCfg.SkipVerify {
		cfg.InsecureSkipVerify = true
	}

	return cfg, nil
}

// classifyConnError classifies connection errors for layered diagnostics (FR-13 rule 8).
func classifyConnError(err error, profile *config.Profile) error {
	errStr := strings.ToLower(err.Error())

	// franz-go reports "broker closed the connection immediately after a
	// request was issued" when the connection is dropped right after connect.
	// Its guess is "SASL missing", but the real cause is almost always one of:
	//   (a) broker listener is SASL_SSL but the client has no tls block,
	//   (b) wrong listener port (PLAINTEXT 9092 vs SSL 9093),
	//   (c) missing/wrong SASL mechanism or credentials.
	// Surface all of these instead of a misleading "auth failed".
	if strings.Contains(errStr, "closed the connection immediately") ||
		strings.Contains(errStr, "sasl is required but not provided") {
		mech, tlsCfg := "", false
		if profile.SASL != nil {
			mech = profile.SASL.Mechanism
		}
		if profile.TLS != nil {
			tlsCfg = true
		}
		return fmt.Errorf("connection dropped immediately after connect: if the broker listener is SASL_SSL you must also configure a tls block; verify mechanism=%q matches the broker and that the port matches the listener type (PLAINTEXT vs SSL); tls_configured=%v: %w",
			mech, tlsCfg, err)
	}

	if isSASLError(errStr) {
		mech := ""
		if profile.SASL != nil {
			mech = profile.SASL.Mechanism
		}
		return fmt.Errorf("SASL authentication failed: %w (hint: check username/password for mechanism %s)", err, mech)
	}
	if isTLSError(errStr) {
		return fmt.Errorf("TLS certificate verification failed: %w (hint: check ca_cert path or set tls.skip_verify=true)", err)
	}
	return fmt.Errorf("TCP connection failed: %w (hint: check bootstrap_servers reachability and firewall)", err)
}

func isSASLError(s string) bool {
	return strings.Contains(s, "sasl") || strings.Contains(s, "authentication") || strings.Contains(s, "auth")
}

func isTLSError(s string) bool {
	return strings.Contains(s, "tls") || strings.Contains(s, "certificate") || strings.Contains(s, "x509")
}

// fetchMetadata requests topic metadata. If topics is nil, all topics are returned.
func (c *Client) fetchMetadata(ctx context.Context, topics []string) (*kmsg.MetadataResponse, error) {
	req := kmsg.NewPtrMetadataRequest()
	if topics != nil {
		req.Topics = make([]kmsg.MetadataRequestTopic, len(topics))
		for i, t := range topics {
			tt := kmsg.NewMetadataRequestTopic()
			name := t
			tt.Topic = &name
			req.Topics[i] = tt
		}
	}
	resp, err := c.kgoClient.Request(ctx, req)
	if err != nil {
		return nil, err
	}
	metaResp, ok := resp.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected metadata response type: %T", resp)
	}
	return metaResp, nil
}

// fetchLatestOffsets returns the latest offset (HWM) for each requested partition of a topic.
func (c *Client) fetchLatestOffsets(ctx context.Context, topic string, partitions []int32) (map[int32]int64, error) {
	if len(partitions) == 0 {
		return nil, nil
	}
	req := kmsg.NewPtrListOffsetsRequest()
	req.Default()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rt.Partitions = make([]kmsg.ListOffsetsRequestTopicPartition, len(partitions))
	for i, p := range partitions {
		pp := kmsg.NewListOffsetsRequestTopicPartition()
		pp.Default()
		pp.Partition = p
		pp.Timestamp = -1 // latest
		rt.Partitions[i] = pp
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{rt}

	resp, err := c.kgoClient.Request(ctx, req)
	if err != nil {
		return nil, err
	}
	offsetResp, ok := resp.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected list offsets response type: %T", resp)
	}
	result := make(map[int32]int64, len(partitions))
	for _, t := range offsetResp.Topics {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.Partitions {
			result[p.Partition] = p.Offset
		}
	}
	return result, nil
}

// ListTopics returns minimal topic metadata (FR-01).
func (c *Client) ListTopics(ctx context.Context, filter string) ([]TopicInfo, error) {
	metaResp, err := c.fetchMetadata(ctx, nil) // all topics
	if err != nil {
		return nil, fmt.Errorf("request metadata: %w", err)
	}

	var topics []TopicInfo
	for _, t := range metaResp.Topics {
		name := ""
		if t.Topic != nil {
			name = *t.Topic
		}
		if name == "" {
			continue
		}
		if filter != "" && !matchFilter(name, filter) {
			continue
		}
		ti := TopicInfo{
			Name:            name,
			PartitionsCount: len(t.Partitions),
			Partitions:      make([]PartitionInfo, 0, len(t.Partitions)),
		}
		partIDs := make([]int32, len(t.Partitions))
		for i, p := range t.Partitions {
			partIDs[i] = p.Partition
		}
		offsetMap, err := c.fetchLatestOffsets(ctx, name, partIDs)
		if err == nil {
			for _, p := range t.Partitions {
				ti.Partitions = append(ti.Partitions, PartitionInfo{
					Partition:    p.Partition,
					LatestOffset: offsetMap[p.Partition],
				})
			}
		} else {
			for _, p := range t.Partitions {
				ti.Partitions = append(ti.Partitions, PartitionInfo{
					Partition:    p.Partition,
					LatestOffset: 0,
				})
			}
		}
		topics = append(topics, ti)
	}
	return topics, nil
}

// DescribeTopic returns metadata for a single topic (FR-01).
func (c *Client) DescribeTopic(ctx context.Context, topic string) (*TopicInfo, error) {
	metaResp, err := c.fetchMetadata(ctx, []string{topic})
	if err != nil {
		return nil, fmt.Errorf("request metadata: %w", err)
	}
	for _, t := range metaResp.Topics {
		name := ""
		if t.Topic != nil {
			name = *t.Topic
		}
		if name != topic {
			continue
		}
		ti := TopicInfo{
			Name:            name,
			PartitionsCount: len(t.Partitions),
			Partitions:      make([]PartitionInfo, 0, len(t.Partitions)),
		}
		partIDs := make([]int32, len(t.Partitions))
		for i, p := range t.Partitions {
			partIDs[i] = p.Partition
		}
		offsetMap, err := c.fetchLatestOffsets(ctx, topic, partIDs)
		if err == nil {
			for _, p := range t.Partitions {
				ti.Partitions = append(ti.Partitions, PartitionInfo{
					Partition:    p.Partition,
					LatestOffset: offsetMap[p.Partition],
				})
			}
		} else {
			for _, p := range t.Partitions {
				ti.Partitions = append(ti.Partitions, PartitionInfo{
					Partition:    p.Partition,
					LatestOffset: 0,
				})
			}
		}
		return &ti, nil
	}
	return nil, fmt.Errorf("topic %q not found", topic)
}

// ListGroups returns consumer group names (FR-02).
func (c *Client) ListGroups(ctx context.Context) ([]string, error) {
	req := kmsg.NewPtrListGroupsRequest()
	resp, err := c.kgoClient.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	lgResp, ok := resp.(*kmsg.ListGroupsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected list groups response type: %T", resp)
	}
	groups := make([]string, 0, len(lgResp.Groups))
	for _, g := range lgResp.Groups {
		groups = append(groups, g.Group)
	}
	return groups, nil
}

// DescribeGroup returns group metadata (FR-02).
func (c *Client) DescribeGroup(ctx context.Context, group string) (*GroupInfo, error) {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{group}
	resp, err := c.kgoClient.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("describe group: %w", err)
	}
	dgResp, ok := resp.(*kmsg.DescribeGroupsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected describe groups response type: %T", resp)
	}
	for _, g := range dgResp.Groups {
		if g.Group != group {
			continue
		}
		info := &GroupInfo{
			Name:  g.Group,
			State: g.State,
		}
		for _, m := range g.Members {
			ci := ConsumerInfo{
				ClientID:   m.ClientID,
				MemberID:   m.MemberID,
				ClientHost: m.ClientHost,
			}
			ci.Assignments = parseMemberAssignments(m.MemberAssignment)
			info.Consumers = append(info.Consumers, ci)
		}
		return info, nil
	}
	return nil, fmt.Errorf("group %q not found", group)
}

// parseMemberAssignments decodes the consumer MemberAssignment bytes into "topic:partition" strings.
func parseMemberAssignments(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var asn kmsg.ConsumerMemberAssignment
	if err := asn.ReadFrom(raw); err != nil {
		return nil
	}
	var out []string
	for _, t := range asn.Topics {
		for _, p := range t.Partitions {
			out = append(out, fmt.Sprintf("%s:%d", t.Topic, p))
		}
	}
	return out
}

// GroupLag returns lag for a consumer group on a topic (FR-02).
// If topic is empty, the first topic with committed offsets is used.
func (c *Client) GroupLag(ctx context.Context, group string, topic string, lagThreshold int64) (*LagInfo, error) {
	// 1. Fetch committed offsets for the group (nil topics = all committed offsets).
	offReq := kmsg.NewPtrOffsetFetchRequest()
	offReq.Group = group
	offRespRaw, err := c.kgoClient.Request(ctx, offReq)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets: %w", err)
	}
	offResp, ok := offRespRaw.(*kmsg.OffsetFetchResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected offset fetch response type: %T", offRespRaw)
	}

	// Normalize: prefer v8+ Groups, fall back to legacy Topics.
	var committedTopics []kmsg.OffsetFetchResponseGroupTopic
	if len(offResp.Groups) > 0 {
		committedTopics = offResp.Groups[0].Topics
	} else {
		committedTopics = make([]kmsg.OffsetFetchResponseGroupTopic, 0, len(offResp.Topics))
		for _, t := range offResp.Topics {
			gt := kmsg.NewOffsetFetchResponseGroupTopic()
			gt.Topic = t.Topic
			for _, p := range t.Partitions {
				gp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
				gp.Partition = p.Partition
				gp.Offset = p.Offset
				gp.LeaderEpoch = p.LeaderEpoch
				gp.Metadata = p.Metadata
				gp.ErrorCode = p.ErrorCode
				gt.Partitions = append(gt.Partitions, gp)
			}
			committedTopics = append(committedTopics, gt)
		}
	}

	// 2. Select the target topic.
	var target *kmsg.OffsetFetchResponseGroupTopic
	for i := range committedTopics {
		if topic == "" || committedTopics[i].Topic == topic {
			target = &committedTopics[i]
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("no lag data for group %q topic %q", group, topic)
	}

	// 3. Fetch HWM (latest offsets) for the target topic's partitions.
	partIDs := make([]int32, len(target.Partitions))
	for i, p := range target.Partitions {
		partIDs[i] = p.Partition
	}
	hwmMap, err := c.fetchLatestOffsets(ctx, target.Topic, partIDs)
	if err != nil {
		return nil, fmt.Errorf("fetch hwm: %w", err)
	}

	// 4. Compute lag per partition.
	li := &LagInfo{
		Topic:      target.Topic,
		Partitions: make([]PartitionLag, 0, len(target.Partitions)),
	}
	var totalLag int64
	for _, p := range target.Partitions {
		hwm := hwmMap[p.Partition]
		committed := p.Offset
		if hwm < 0 {
			hwm = 0
		}
		var lag int64
		if committed < 0 {
			// No committed offset: everything from the start is lag.
			lag = hwm
		} else {
			lag = hwm - committed
			if lag < 0 {
				lag = 0
			}
		}
		li.Partitions = append(li.Partitions, PartitionLag{
			Partition: p.Partition,
			Committed: committed,
			HWM:       hwm,
			Lag:       lag,
		})
		totalLag += lag
	}
	li.TotalLag = totalLag
	if lagThreshold > 0 && totalLag > lagThreshold {
		li.HighLag = true
	}
	return li, nil
}

// KgoClient returns the underlying kgo.Client for direct access (scan/produce).
func (c *Client) KgoClient() *kgo.Client {
	return c.kgoClient
}

// Profile returns the associated profile.
func (c *Client) Profile() *config.Profile {
	return c.profile
}

// Close closes the underlying client.
func (c *Client) Close() {
	c.kgoClient.Close()
}

// matchFilter does case-insensitive substring matching.
func matchFilter(name, filter string) bool {
	if filter == "" {
		return true
	}
	return strings.Contains(strings.ToLower(name), strings.ToLower(filter))
}
