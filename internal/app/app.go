package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"kafka-pilot/internal/audit"
	"kafka-pilot/internal/config"
	"kafka-pilot/internal/filter"
	"kafka-pilot/internal/kafka"
	"kafka-pilot/internal/produce"
	"kafka-pilot/internal/scan"
)

// App is the core application orchestrator, shared by CLI and HTTP service.
type App struct {
	Config *config.Manager
	Kafka  *kafka.Pool
	Audit  *audit.Store
	Closed bool

	// Settings
	LagThreshold    int64
	SnapshotEnabled bool
}

// New creates a new App with default paths.
func New() (*App, error) {
	cfgMgr, err := config.NewManager()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	appData := config.AppDataDir()
	auditPath := filepath.Join(appData, "audit.db")
	store, err := audit.Open(auditPath)
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}

	// Recover crashed ops and auto-end sessions
	store.RecoverCrashedOps()
	store.AutoEndSessions()

	return &App{
		Config:          cfgMgr,
		Kafka:           kafka.NewPool(),
		Audit:           store,
		LagThreshold:    10000,
		SnapshotEnabled: true,
	}, nil
}

// NewWithPath creates an App with explicit paths (for testing).
func NewWithPath(configPath, auditPath string) (*App, error) {
	cfgMgr, err := config.NewManagerWithPath(configPath)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	store, err := audit.Open(auditPath)
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}
	store.RecoverCrashedOps()
	store.AutoEndSessions()
	return &App{
		Config:          cfgMgr,
		Kafka:           kafka.NewPool(),
		Audit:           store,
		LagThreshold:    10000,
		SnapshotEnabled: true,
	}, nil
}

// Close cleans up resources.
func (a *App) Close() {
	if a.Closed {
		return
	}
	a.Closed = true
	a.Kafka.Close()
	a.Audit.Close()
}

// GenerateOpID generates a unique operation ID.
func GenerateOpID() string {
	return "op_" + strings.ToLower(ulid.Make().String())
}

// OpContext holds metadata for an operation.
type OpContext struct {
	OpID      string
	SessionID string
	Source    string // "ai" or "human"
	Actor     string
	Intent    string
	Command   string
	StartedAt int64
}

// NewOpContext creates an op context and starts the audit record.
func (a *App) NewOpContext(sessionID, source, actor, intent, command string) *OpContext {
	opID := GenerateOpID()
	now := time.Now().UnixMilli()

	// Truncate command if > 2KB (FR-09)
	commandTrunc := false
	if len(command) > 2048 {
		command = command[:2048]
		commandTrunc = true
	}

	op := &audit.Op{
		OpID:         opID,
		SessionID:    sessionID,
		Source:       source,
		Actor:        actor,
		Intent:       intent,
		Command:      command,
		CommandTrunc: commandTrunc,
		StartedAt:    now,
		Status:       "running",
	}
	a.Audit.StartOp(op)

	return &OpContext{
		OpID:      opID,
		SessionID: sessionID,
		Source:    source,
		Actor:     actor,
		Intent:    intent,
		Command:   command,
		StartedAt: now,
	}
}

// FinishOp completes the audit record.
func (a *App) FinishOp(opCtx *OpContext, status string, resultSummary interface{}, warnings []string, opErr error) {
	warningsJSON := "[]"
	if len(warnings) > 0 {
		b, _ := json.Marshal(warnings)
		warningsJSON = string(b)
	}

	resultJSON := "{}"
	if resultSummary != nil {
		b, _ := json.Marshal(resultSummary)
		resultJSON = string(b)
	}

	errStr := ""
	if opErr != nil {
		errJSON, _ := json.Marshal(map[string]string{
			"code":    status,
			"message": opErr.Error(),
		})
		errStr = string(errJSON)
	}

	a.Audit.FinishOp(opCtx.OpID, status, time.Since(time.UnixMilli(opCtx.StartedAt)).Milliseconds(), resultJSON, warningsJSON, errStr)
}

// --- Scan ---

// ScanRequest holds scan parameters from CLI/UI.
type ScanRequest struct {
	Profile    string
	Topic      string
	Partitions []int32
	// KeyPartition is an EXPLICIT opt-in: narrow the scan to the single
	// partition derived from an exact key via the standard murmur2 hasher.
	// Default false -- see FR-05 for why an implicit hint can silently lose data.
	KeyPartition   bool
	Last           int
	Since          time.Time
	Until          time.Time
	HasTimeWindow  bool
	OffsetFrom     int64
	OffsetTo       int64
	HasOffsetRange bool
	TimeoutSec     int

	// Filters
	Key        string
	KeyRegex   string
	Keyword    string
	IgnoreCase bool
	Expr       string
}

// ScanResult is the JSON output for scan.
type ScanResult struct {
	OpID              string                `json:"opId"`
	Topic             string                `json:"topic"`
	Total             int                   `json:"total"`
	Matched           int                   `json:"matched"`
	MatchedRaw        int                   `json:"matched_raw,omitempty"`
	Truncated         bool                  `json:"truncated"`
	Warnings          []string              `json:"warnings,omitempty"`
	ElapsedMs         int64                 `json:"elapsedMs"`
	Messages          []*filter.Result      `json:"messages,omitempty"`
	PartitionRanges   []scan.PartitionRange `json:"partition_ranges,omitempty"`
	SkippedNonUTF8Key int                   `json:"skippedNonUtf8Key,omitempty"`
	// KeyPartition / KeyPartitionCount are present only when the explicit
	// key-location switch was used, so the UI can state exactly which single
	// partition was scanned (and how many were NOT checked).
	KeyPartition      *int32 `json:"key_partition,omitempty"`
	KeyPartitionCount int    `json:"key_partition_count,omitempty"`
}

// Scan executes a scan operation.
func (a *App) Scan(ctx context.Context, opCtx *OpContext, req *ScanRequest) (*ScanResult, error) {
	// Resolve profile
	profile, err := a.Config.Resolve(req.Profile)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	// Get kafka client
	kc, err := a.Kafka.Get(profile)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	// Build filter
	filterCfg := filter.FilterConfig{
		Key:        req.Key,
		KeyRegex:   req.KeyRegex,
		Keyword:    req.Keyword,
		IgnoreCase: req.IgnoreCase,
		Expr:       req.Expr,
	}
	f, err := filter.New(filterCfg)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	// Build scan request
	scanReq := &scan.Request{
		Topic:          req.Topic,
		Partitions:     req.Partitions,
		Last:           req.Last,
		Since:          req.Since,
		Until:          req.Until,
		HasTimeWindow:  req.HasTimeWindow,
		OffsetFrom:     req.OffsetFrom,
		OffsetTo:       req.OffsetTo,
		HasOffsetRange: req.HasOffsetRange,
		Timeout:        time.Duration(req.TimeoutSec) * time.Second,
	}

	// Explicit key-location switch (FR-05). This is deliberately opt-in: a
	// wrong partition hint silently drops messages, which is the worst failure
	// mode for a query tool. Only a non-empty EXACT key is eligible; regex /
	// keyword / jq filters cannot be narrowed this way.
	var keyPartPtr *int32
	keyPartCount := 0
	if req.KeyPartition {
		if req.Key == "" || req.KeyRegex != "" {
			err := fmt.Errorf("key-partition requires a non-empty exact key and cannot be combined with key-regex")
			a.FinishOp(opCtx, "error", nil, nil, err)
			return nil, err
		}
		info, err := a.DescribeTopic(ctx, req.Profile, req.Topic)
		if err != nil {
			a.FinishOp(opCtx, "error", nil, nil, err)
			return nil, err
		}
		if info.PartitionsCount <= 0 {
			err := fmt.Errorf("topic %s has no partitions", req.Topic)
			a.FinishOp(opCtx, "error", nil, nil, err)
			return nil, err
		}
		p := int32(kafka.KeyPartition([]byte(req.Key), info.PartitionsCount))
		keyPartPtr = &p
		keyPartCount = info.PartitionsCount
		// The computed partition wins over any explicit partition selection;
		// the Web UI disables its partition picker while the switch is on.
		scanReq.Partitions = []int32{p}
	}

	if err := scan.ValidateRequest(scanReq); err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	scanner := scan.New(kc.KgoClient())
	result, err := scanner.Scan(ctx, scanReq, f)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	result.OpID = opCtx.OpID
	if keyPartPtr != nil {
		// Machine-readable marker kept in the API / CLI / audit log; the Web UI
		// expands it into a plain-language statement of what was NOT scanned.
		result.Warnings = append(result.Warnings, "key-partition")
	}

	// Save snapshot if enabled and there are matches
	if a.SnapshotEnabled && len(result.Messages) > 0 {
		a.saveSnapshot(opCtx, result)
	}

	// Determine status
	status := "ok"
	if result.Truncated {
		status = "truncated"
	}

	summary := map[string]interface{}{
		"topic":     req.Topic,
		"total":     result.Total,
		"matched":   result.Matched,
		"elapsedMs": result.ElapsedMs,
	}
	if keyPartPtr != nil {
		summary["key_partition"] = *keyPartPtr
		summary["key_partition_count"] = keyPartCount
	}
	a.FinishOp(opCtx, status, summary, result.Warnings, nil)

	return &ScanResult{
		OpID:              result.OpID,
		Topic:             result.Topic,
		Total:             result.Total,
		Matched:           result.Matched,
		MatchedRaw:        result.MatchedRaw,
		Truncated:         result.Truncated,
		Warnings:          result.Warnings,
		ElapsedMs:         result.ElapsedMs,
		Messages:          result.Messages,
		PartitionRanges:   result.PartitionRanges,
		SkippedNonUTF8Key: result.SkippedNonUTF8Key,
		KeyPartition:      keyPartPtr,
		KeyPartitionCount: keyPartCount,
	}, nil
}

func (a *App) saveSnapshot(opCtx *OpContext, result *scan.Result) {
	// Bound: <=100 messages and <=1MB serialized
	msgs := result.Messages
	truncated := false
	if len(msgs) > 100 {
		msgs = msgs[:100]
		truncated = true
	}
	data, err := json.Marshal(msgs)
	if err != nil {
		return
	}
	if len(data) > 1024*1024 {
		// Truncate to fit 1MB - just store first N that fit
		for len(msgs) > 0 {
			msgs = msgs[:len(msgs)-1]
			data, err = json.Marshal(msgs)
			if err != nil {
				return
			}
			if len(data) <= 1024*1024 {
				truncated = true
				break
			}
		}
	}
	snap := &audit.Snapshot{
		OpID:      opCtx.OpID,
		SessionID: opCtx.SessionID,
		CreatedAt: time.Now().UnixMilli(),
		Messages:  data,
		Truncated: truncated,
	}
	a.Audit.SaveSnapshot(snap)
}

// --- Produce ---

// ProduceSingle executes a single-message produce.
func (a *App) ProduceSingle(ctx context.Context, opCtx *OpContext, profileName, topic string, key, value []byte, headers []string) (map[string]interface{}, error) {
	profile, err := a.Config.Resolve(profileName)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	kc, err := a.Kafka.Get(profile)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	hdrs, err := produce.ParseHeaders(headers)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	prod := produce.New(kc.KgoClient())
	req := &produce.SingleRequest{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: hdrs,
	}

	part, offset, err := prod.ProduceSingle(ctx, req)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	result := map[string]interface{}{
		"topic":     topic,
		"partition": part,
		"offset":    offset,
		"produced":  1,
	}

	a.FinishOp(opCtx, "ok", result, nil, nil)
	return result, nil
}

// ProduceBatch executes a script-based batch produce.
func (a *App) ProduceBatch(ctx context.Context, opCtx *OpContext, profileName string, req *produce.BatchRequest, progressFn func(produced, failed int64)) (*produce.BatchResult, error) {
	profile, err := a.Config.Resolve(profileName)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}
	kc, err := a.Kafka.Get(profile)
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	prod := produce.New(kc.KgoClient())
	result, err := prod.ProduceBatch(ctx, req, func(produced, failed int64) {
		// Update audit progress in-place (FR-09 rule 3)
		summary, _ := json.Marshal(map[string]interface{}{
			"produced": produced,
			"failed":   failed,
		})
		a.Audit.UpdateOpProgress(opCtx.OpID, string(summary))
		if progressFn != nil {
			progressFn(produced, failed)
		}
	})
	if err != nil {
		a.FinishOp(opCtx, "error", nil, nil, err)
		return nil, err
	}

	status := "ok"
	if result.Cancelled {
		status = "cancelled"
	}

	a.FinishOp(opCtx, status, result, nil, nil)
	return result, nil
}

// --- Inspect ---

// ListTopics lists topics (FR-01).
func (a *App) ListTopics(ctx context.Context, profileName, filter string) ([]kafka.TopicInfo, error) {
	profile, err := a.Config.Resolve(profileName)
	if err != nil {
		return nil, err
	}
	kc, err := a.Kafka.Get(profile)
	if err != nil {
		return nil, err
	}
	return kc.ListTopics(ctx, filter)
}

// DescribeTopic describes a topic (FR-01).
func (a *App) DescribeTopic(ctx context.Context, profileName, topic string) (*kafka.TopicInfo, error) {
	profile, err := a.Config.Resolve(profileName)
	if err != nil {
		return nil, err
	}
	kc, err := a.Kafka.Get(profile)
	if err != nil {
		return nil, err
	}
	return kc.DescribeTopic(ctx, topic)
}

// KeyPartitionHint returns the partition a standard Kafka producer would pick
// for key, together with the topic's current partition count. It is a
// read-only display helper for the explicit key-location switch (FR-05); the
// result is a hint, not a guarantee that records with this key live there.
func (a *App) KeyPartitionHint(ctx context.Context, profileName, topic, key string) (int, int, error) {
	info, err := a.DescribeTopic(ctx, profileName, topic)
	if err != nil {
		return 0, 0, err
	}
	if info.PartitionsCount <= 0 {
		return 0, 0, fmt.Errorf("topic %s has no partitions", topic)
	}
	return kafka.KeyPartition([]byte(key), info.PartitionsCount), info.PartitionsCount, nil
}

// ListGroups lists consumer groups (FR-02).
func (a *App) ListGroups(ctx context.Context, profileName string) ([]string, error) {
	profile, err := a.Config.Resolve(profileName)
	if err != nil {
		return nil, err
	}
	kc, err := a.Kafka.Get(profile)
	if err != nil {
		return nil, err
	}
	return kc.ListGroups(ctx)
}

// GroupLag gets lag for a consumer group (FR-02).
func (a *App) GroupLag(ctx context.Context, profileName, group, topic string) (*kafka.LagInfo, error) {
	profile, err := a.Config.Resolve(profileName)
	if err != nil {
		return nil, err
	}
	kc, err := a.Kafka.Get(profile)
	if err != nil {
		return nil, err
	}
	return kc.GroupLag(ctx, group, topic, a.LagThreshold)
}

// --- Session ---

// CreateSession creates a new session (FR-10).
func (a *App) CreateSession(label string) (string, error) {
	sessionID := audit.GenerateSessionID()
	// Ensure uniqueness
	for {
		if existing, _ := a.Audit.GetSession(sessionID); existing == nil {
			break
		}
		sessionID = audit.GenerateSessionID()
	}
	err := a.Audit.CreateSession(sessionID, label, time.Now().UnixMilli())
	if err != nil {
		return "", err
	}
	return sessionID, nil
}

// EndSession ends a session (FR-10).
func (a *App) EndSession(sessionID string) error {
	return a.Audit.EndSession(sessionID, time.Now().UnixMilli(), false)
}

// ListSessions lists sessions (FR-10).
func (a *App) ListSessions() ([]*audit.Session, error) {
	return a.Audit.ListSessions(100)
}

// --- Audit ---

// ListOps lists operations (FR-09).
func (a *App) ListOps(sessionID, source string, since int64) ([]*audit.Op, error) {
	return a.Audit.ListOps(sessionID, source, since, 100)
}

// GetOp gets a single operation (FR-09).
func (a *App) GetOp(opID string) (*audit.Op, error) {
	return a.Audit.GetOp(opID)
}

// GetSnapshot gets the snapshot for an op (FR-09, replay).
func (a *App) GetSnapshot(opID string) (*audit.Snapshot, error) {
	return a.Audit.GetSnapshot(opID)
}

// ClearAudit clears audit data (FR-09).
func (a *App) ClearAudit(before int64) error {
	return a.Audit.ClearAudit(before)
}

// --- Profile ---

// AddProfile adds a profile (FR-13).
func (a *App) AddProfile(p config.Profile) error {
	return a.Config.Add(p)
}

// UpdateProfile updates a profile and drops any cached Kafka client for it,
// so the next operation reconnects with the fresh SASL/TLS settings.
func (a *App) UpdateProfile(name string, p config.Profile) error {
	if err := a.Config.Update(name, p); err != nil {
		return err
	}
	a.Kafka.Invalidate(name)
	return nil
}

// RemoveProfile removes a profile.
func (a *App) RemoveProfile(name string) error {
	return a.Config.Remove(name)
}

// ListProfiles lists profiles.
func (a *App) ListProfiles() []config.Profile {
	return a.Config.List()
}

// SetDefaultProfile sets default.
func (a *App) SetDefaultProfile(name string) error {
	return a.Config.SetDefault(name)
}

// GetActor returns the current actor name.
func GetActor() string {
	if user := os.Getenv("USERNAME"); user != "" {
		return user
	}
	if user := os.Getenv("USER"); user != "" {
		return user
	}
	return "unknown"
}
