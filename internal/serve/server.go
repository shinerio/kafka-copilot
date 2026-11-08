package serve

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"kafka-pilot/internal/app"
	"kafka-pilot/internal/audit"
	"kafka-pilot/internal/config"
	"kafka-pilot/internal/produce"
)

//go:embed dist/*
var distFS embed.FS

// Server is the HTTP API server for Kafka Pilot.
type Server struct {
	app        *app.App
	mux        *http.ServeMux
	distRoot   fs.FS
	fileServer http.Handler
}

// NewServer creates a new Server wired to the given App.
func NewServer(a *app.App) *Server {
	s := &Server{
		app: a,
		mux: http.NewServeMux(),
	}
	s.distRoot, _ = fs.Sub(distFS, "dist")
	s.fileServer = http.FileServer(http.FS(s.distRoot))
	s.registerRoutes()
	return s
}

// ListenAndServe starts the HTTP server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s.mux)
}

// registerRoutes wires all API endpoints and static file handlers.
func (s *Server) registerRoutes() {
	// Health
	s.mux.HandleFunc("GET /api/health", s.handleHealth)

	// Schema (machine-readable self-description: command list + exit codes)
	s.mux.HandleFunc("GET /api/schema", s.handleSchema)

	// Profiles
	s.mux.HandleFunc("GET /api/profiles", s.handleListProfiles)
	s.mux.HandleFunc("POST /api/profiles", s.handleAddProfile)
	s.mux.HandleFunc("GET /api/profiles/{name}", s.handleGetProfile)
	s.mux.HandleFunc("PUT /api/profiles/{name}", s.handleUpdateProfile)
	s.mux.HandleFunc("DELETE /api/profiles/{name}", s.handleRemoveProfile)
	s.mux.HandleFunc("PUT /api/profiles/{name}/default", s.handleSetDefaultProfile)

	// Topics
	s.mux.HandleFunc("GET /api/topics", s.handleListTopics)
	s.mux.HandleFunc("GET /api/topics/{topic}", s.handleDescribeTopic)

	// Scan & Produce
	// Read-only helper: compute which partition a standard producer would pick
	// for an exact key. Used by the Web UI to show the hint BEFORE scanning.
	s.mux.HandleFunc("GET /api/key-partition", s.handleKeyPartition)
	s.mux.HandleFunc("POST /api/scan", s.handleScan)
	s.mux.HandleFunc("POST /api/produce", s.handleProduce)
	s.mux.HandleFunc("POST /api/produce/batch", s.handleProduceBatch)

	// Groups
	s.mux.HandleFunc("GET /api/groups", s.handleListGroups)
	s.mux.HandleFunc("GET /api/groups/{group}/lag", s.handleGroupLag)

	// Sessions
	s.mux.HandleFunc("GET /api/sessions", s.handleListSessions)
	s.mux.HandleFunc("GET /api/sessions/{id}", s.handleGetSession)
	s.mux.HandleFunc("DELETE /api/sessions", s.handleDeleteSessions)

	// Audit
	s.mux.HandleFunc("GET /api/audit/ops", s.handleListOps)
	s.mux.HandleFunc("DELETE /api/audit/ops", s.handleDeleteOps)
	s.mux.HandleFunc("GET /api/audit/ops/{op_id}", s.handleGetOp)
	s.mux.HandleFunc("GET /api/audit/ops/{op_id}/snapshot", s.handleGetSnapshot)
	s.mux.HandleFunc("GET /api/audit/stream", s.handleAuditStream)
	s.mux.HandleFunc("DELETE /api/audit", s.handleClearAudit)

	// Static files / SPA fallback (catch-all for GET)
	s.mux.HandleFunc("GET /", s.handleStatic)
}

// =============================================================================
// Helpers
// =============================================================================

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	// Normalize nil slices to [] so list endpoints always return a JSON array
	// (Go encodes a nil slice as null, which breaks Array.isArray on the UI).
	if v != nil {
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			v = reflect.MakeSlice(rv.Type(), 0, 0).Interface()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAPIError(w http.ResponseWriter, httpStatus, code int, message, hint string) {
	resp := map[string]interface{}{"code": code, "message": message}
	if hint != "" {
		resp["hint"] = hint
	}
	writeJSON(w, httpStatus, resp)
}

// classifyError maps an application error to HTTP status + error code + hint.
func classifyError(err error) (httpStatus int, code int, hint string) {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound, 2, ""
	case strings.Contains(msg, "already exists"):
		return http.StatusConflict, 2, ""
	case strings.Contains(msg, "connection") || strings.Contains(msg, "tcp") ||
		strings.Contains(msg, "sasl") || strings.Contains(msg, "tls"):
		return http.StatusServiceUnavailable, 3, "check profile configuration and Kafka cluster availability"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return http.StatusGatewayTimeout, 4, "operation timed out"
	case strings.Contains(msg, "invalid") || strings.Contains(msg, "required") ||
		strings.Contains(msg, "exceeds") || strings.Contains(msg, "unsupported"):
		return http.StatusBadRequest, 2, "check request parameters"
	default:
		return http.StatusInternalServerError, 5, ""
	}
}

func writeAppError(w http.ResponseWriter, err error) {
	hs, code, hint := classifyError(err)
	writeAPIError(w, hs, code, err.Error(), hint)
}

// parseFlexibleTime accepts several timestamp shapes so the UI (native
// datetime-local) and CLI users are not forced into one exact spelling:
//   - Unix millis (numeric string)
//   - RFC3339 / RFC3339Nano with "+08:00" or "Z"
//   - compact offset "+0800" (legacy UI builds emitted this)
//   - zone-less local wall time "2026-11-07T19:23:00", "...T19:23",
//     "2026-11-07 19:23:00", "2026-11-07"
func parseFlexibleTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(n), nil
	}
	zoned := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z0700", "2006-01-02T15:04:05.999999999Z0700"}
	for _, layout := range zoned {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	local := []string{"2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02"}
	for _, layout := range local {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time (use RFC3339 or local datetime): %q", s)
}

// parseTimeParam returns Unix millis; "" maps to 0 (no filter).
func parseTimeParam(s string) (int64, error) {
	if strings.TrimSpace(s) == "" {
		return 0, nil
	}
	t, err := parseFlexibleTime(s)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

// derefInt64 returns *v if non-nil, else def.
func derefInt64(v *int64, def int64) int64 {
	if v == nil {
		return def
	}
	return *v
}

func maskProfilePassword(p *config.Profile) {
	if p.SASL != nil && p.SASL.Password != "" {
		p.SASL.Password = "****"
	}
}

// =============================================================================
// Health
// =============================================================================

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleSchema mirrors `kp schema`: machine-readable command list + exit codes,
// so REST clients (and agents) can discover capabilities without hardcoding them.
func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"commands": []string{
			"profile", "topic", "group", "scan", "produce",
			"session", "audit", "schema", "key-partition",
			"serve", "desktop", "stop", "restart",
		},
		"exit_codes": map[string]int{
			"ok":          0,
			"param_error": 2,
			"conn_error":  3,
			"timeout":     4,
			"internal":    5,
		},
	})
}

// =============================================================================
// Profiles
// =============================================================================

type profileRequest struct {
	Name             string       `json:"name,omitempty"`
	BootstrapServers []string     `json:"bootstrap_servers"`
	SASL             *config.SASL `json:"sasl,omitempty"`
	TLS              *config.TLS  `json:"tls,omitempty"`
}

func (s *Server) handleListProfiles(w http.ResponseWriter, r *http.Request) {
	profiles := s.app.ListProfiles() // passwords already masked by Manager.List
	writeJSON(w, http.StatusOK, profiles)
}

func (s *Server) handleAddProfile(w http.ResponseWriter, r *http.Request) {
	var body profileRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, "invalid JSON: "+err.Error(), "")
		return
	}
	if body.Name == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "name is required", "")
		return
	}
	if len(body.BootstrapServers) == 0 {
		writeAPIError(w, http.StatusBadRequest, 2, "bootstrap_servers is required", "")
		return
	}
	p := config.Profile{
		Name:             body.Name,
		BootstrapServers: body.BootstrapServers,
		SASL:             body.SASL,
		TLS:              body.TLS,
	}
	if err := s.app.AddProfile(p); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "added", "profile": p.Name})
}

func (s *Server) handleGetProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	p, err := s.app.Config.Get(name)
	if err != nil {
		writeAppError(w, err)
		return
	}
	maskProfilePassword(p)
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) handleUpdateProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var body profileRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, "invalid JSON: "+err.Error(), "")
		return
	}
	if len(body.BootstrapServers) == 0 {
		writeAPIError(w, http.StatusBadRequest, 2, "bootstrap_servers is required", "")
		return
	}
	p := config.Profile{
		Name:             name,
		BootstrapServers: body.BootstrapServers,
		SASL:             body.SASL,
		TLS:              body.TLS,
	}
	if err := s.app.UpdateProfile(name, p); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated", "profile": name})
}

func (s *Server) handleRemoveProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.app.RemoveProfile(name); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed", "profile": name})
}

func (s *Server) handleSetDefaultProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.app.SetDefaultProfile(name); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "default_set", "profile": name})
}

// =============================================================================
// Topics
// =============================================================================

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	filter := r.URL.Query().Get("filter")
	profile := r.URL.Query().Get("profile")
	topics, err := s.app.ListTopics(r.Context(), profile, filter)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, topics)
}

func (s *Server) handleDescribeTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	profile := r.URL.Query().Get("profile")
	info, err := s.app.DescribeTopic(r.Context(), profile, topic)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

// =============================================================================
// Key partition hint (read-only helper for the explicit key-location switch)
// =============================================================================

func (s *Server) handleKeyPartition(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	key := r.URL.Query().Get("key")
	profile := r.URL.Query().Get("profile")
	if topic == "" || key == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "topic and key are required", "")
		return
	}
	partition, count, err := s.app.KeyPartitionHint(r.Context(), profile, topic, key)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"topic":           topic,
		"key":             key,
		"partition":       partition,
		"partition_count": count,
		"algorithm":       "murmur2(key) & 0x7fffffff % partition_count",
	})
}

// =============================================================================
// Scan
// =============================================================================

type scanRequest struct {
	Profile      string  `json:"profile,omitempty"`
	Topic        string  `json:"topic"`
	Last         int     `json:"last,omitempty"`
	Since        string  `json:"since,omitempty"`
	Until        string  `json:"until,omitempty"`
	Key          string  `json:"key,omitempty"`
	KeyRegex     string  `json:"key_regex,omitempty"`
	Keyword      string  `json:"keyword,omitempty"`
	Expr         string  `json:"expr,omitempty"`
	Partitions   []int32 `json:"partitions,omitempty"`
	KeyPartition bool    `json:"key_partition,omitempty"`
	// OffsetFrom/OffsetTo mirror the CLI --offset-from/--offset-to pair.
	// Pointers are used so an explicit 0 can be distinguished from "absent".
	OffsetFrom *int64 `json:"offset_from,omitempty"`
	OffsetTo   *int64 `json:"offset_to,omitempty"`
	// IgnoreCase mirrors the CLI --ignore-case flag (default true). A pointer
	// lets the client explicitly request case-sensitive matching (false).
	IgnoreCase *bool `json:"ignore_case,omitempty"`
	Timeout    int    `json:"timeout,omitempty"`
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	var body scanRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, "invalid JSON: "+err.Error(), "")
		return
	}
	if body.Topic == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "topic is required", "")
		return
	}

	req := &app.ScanRequest{
		Profile:      body.Profile,
		Topic:        body.Topic,
		Last:         body.Last,
		Key:          body.Key,
		KeyRegex:     body.KeyRegex,
		Keyword:      body.Keyword,
		Expr:         body.Expr,
		Partitions:   body.Partitions,
		KeyPartition: body.KeyPartition,
		TimeoutSec:   body.Timeout,
		IgnoreCase:   body.IgnoreCase == nil || *body.IgnoreCase,
	}

	if body.Since != "" {
		t, err := parseFlexibleTime(body.Since)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, 2, "invalid since format: "+err.Error(), "")
			return
		}
		req.Since = t
		req.HasTimeWindow = true
	}
	if body.Until != "" {
		t, err := parseFlexibleTime(body.Until)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, 2, "invalid until format: "+err.Error(), "")
			return
		}
		req.Until = t
		req.HasTimeWindow = true
	}

	if body.OffsetFrom != nil || body.OffsetTo != nil {
		req.OffsetFrom = derefInt64(body.OffsetFrom, 0)
		req.OffsetTo = derefInt64(body.OffsetTo, 0)
		req.HasOffsetRange = true
		req.Last = 0 // offset range and last are mutually exclusive
	}

	if req.Last == 0 && !req.HasTimeWindow && !req.HasOffsetRange {
		req.Last = 100 // default backscan
	}

	// Create audit op (source=human)
	command := fmt.Sprintf("HTTP scan topic=%s", body.Topic)
	if body.Key != "" {
		command += " key=" + body.Key
	}
	if body.KeyPartition {
		command += " key-partition=true"
	}
	if body.Keyword != "" {
		command += " keyword=" + body.Keyword
	}
	opCtx := s.app.NewOpContext("", "human", app.GetActor(), "", command)

	result, err := s.app.Scan(r.Context(), opCtx, req)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// =============================================================================
// Produce
// =============================================================================

type produceRequest struct {
	Profile string          `json:"profile,omitempty"`
	Topic   string          `json:"topic"`
	Key     string          `json:"key"`
	Value   string          `json:"value"`
	Headers json.RawMessage `json:"headers,omitempty"`
}

// parseProduceHeaders normalizes the two header shapes our clients send into
// the "k=v" []string form consumed by app.ProduceSingle / produce.ParseHeaders:
//   - object: {"traceId":"abc","source":"ui"}  — Web UI (preferred, no '=' ambiguity)
//   - array:  ["traceId=abc","source=ui"]      — CLI-style, kept for compatibility
//
// A typed []string field used to reject the UI's object with
// "cannot unmarshal object into Go struct field ... of type []string".
func parseProduceHeaders(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}

	if strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("headers object is invalid: %w", err)
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys) // deterministic header order

		out := make([]string, 0, len(keys))
		for _, k := range keys {
			if k == "" {
				return nil, fmt.Errorf("header name must not be empty")
			}
			if strings.Contains(k, "=") {
				return nil, fmt.Errorf("header name %q must not contain '='", k)
			}
			var v string
			if err := json.Unmarshal(obj[k], &v); err != nil {
				// Numbers / booleans / null are accepted as their JSON text.
				v = string(obj[k])
				if v == "null" {
					v = ""
				}
			}
			out = append(out, k+"="+v)
		}
		return out, nil
	}

	var arr []string
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, fmt.Errorf("headers must be an object {\"k\":\"v\"} or an array [\"k=v\"]")
	}
	return arr, nil
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	var body produceRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, "invalid JSON: "+err.Error(), "")
		return
	}
	if body.Topic == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "topic is required", "")
		return
	}
	if body.Value == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "value is required", "")
		return
	}
	if body.Key == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "key is required",
			"the target partition is derived from the key")
		return
	}
	headers, err := parseProduceHeaders(body.Headers)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, err.Error(),
			"send headers as an object {\"name\":\"value\"} or an array [\"name=value\"]")
		return
	}

	command := fmt.Sprintf("HTTP produce topic=%s key=%s", body.Topic, body.Key)
	opCtx := s.app.NewOpContext("", "human", app.GetActor(), "", command)

	result, err := s.app.ProduceSingle(r.Context(), opCtx, body.Profile, body.Topic,
		[]byte(body.Key), []byte(body.Value), headers)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

type batchProduceRequest struct {
	Profile string `json:"profile,omitempty"`
	Topic   string `json:"topic"`
	// Script is a path to a .js file on the server (CLI-style callers).
	// ScriptContent is inline JS source, which is what the Web UI sends after
	// reading a user-selected file with FileReader. Either one is required.
	Script        string `json:"script,omitempty"`
	ScriptContent string `json:"script_content,omitempty"`
	Count         int    `json:"count"`
	Concurrency   int    `json:"concurrency,omitempty"`
	Rate          int    `json:"rate,omitempty"`
	DryRun        bool   `json:"dry_run,omitempty"`
}

func (s *Server) handleProduceBatch(w http.ResponseWriter, r *http.Request) {
	var body batchProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, "invalid JSON: "+err.Error(), "")
		return
	}
	if body.Topic == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "topic is required", "")
		return
	}
	if body.Script == "" && body.ScriptContent == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "script is required",
			"send inline JS source in script_content, or a server-side file path in script")
		return
	}

	req := &produce.BatchRequest{
		Topic:         body.Topic,
		ScriptPath:    body.Script,
		ScriptContent: body.ScriptContent,
		Count:         body.Count,
		Concurrency:   body.Concurrency,
		Rate:          body.Rate,
		DryRun:        body.DryRun,
	}
	if req.Concurrency == 0 {
		req.Concurrency = 8
	}
	if req.Count <= 0 {
		req.Count = 100
	}

	command := fmt.Sprintf("HTTP produce batch topic=%s count=%d", body.Topic, body.Count)
	opCtx := s.app.NewOpContext("", "human", app.GetActor(), "", command)

	result, err := s.app.ProduceBatch(r.Context(), opCtx, body.Profile, req, nil)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

// =============================================================================
// Groups
// =============================================================================

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
	profile := r.URL.Query().Get("profile")
	groups, err := s.app.ListGroups(r.Context(), profile)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, groups)
}

func (s *Server) handleGroupLag(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	topic := r.URL.Query().Get("topic")
	profile := r.URL.Query().Get("profile")
	lag, err := s.app.GroupLag(r.Context(), profile, group, topic)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lag)
}

// =============================================================================
// Sessions
// =============================================================================

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := s.app.ListSessions()
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sess, err := s.app.Audit.GetSession(id)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if sess == nil {
		writeAPIError(w, http.StatusNotFound, 2, "session not found", "")
		return
	}
	ops, _ := s.app.ListOps(id, "", 0)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"session": sess,
		"ops":     ops,
	})
}

// =============================================================================
// Audit
// =============================================================================

func (s *Server) handleListOps(w http.ResponseWriter, r *http.Request) {
	session := r.URL.Query().Get("session")
	source := r.URL.Query().Get("source")
	since, err := parseTimeParam(r.URL.Query().Get("since"))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, err.Error(), "")
		return
	}
	ops, err := s.app.ListOps(session, source, since)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ops)
}

func (s *Server) handleGetOp(w http.ResponseWriter, r *http.Request) {
	opID := r.PathValue("op_id")
	op, err := s.app.GetOp(opID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, op)
}

func (s *Server) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	opID := r.PathValue("op_id")
	snap, err := s.app.GetSnapshot(opID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if snap == nil {
		writeAPIError(w, http.StatusNotFound, 2, "snapshot not found for op "+opID, "")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleClearAudit(w http.ResponseWriter, r *http.Request) {
	before, err := parseTimeParam(r.URL.Query().Get("before"))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, err.Error(), "")
		return
	}
	if err := s.app.ClearAudit(before); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cleared"})
}

// handleDeleteOps removes explicitly selected audit operations (and their
// snapshots). Used by the observation page's multi-select delete action.
func (s *Server) handleDeleteOps(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OpIDs []string `json:"op_ids"`
		IDs   []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, "invalid JSON body: "+err.Error(), "")
		return
	}
	ids := body.OpIDs
	if len(ids) == 0 {
		ids = body.IDs
	}
	if len(ids) == 0 {
		writeAPIError(w, http.StatusBadRequest, 2, "op_ids is required", "select at least one operation")
		return
	}
	n, err := s.app.Audit.DeleteOps(ids)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "deleted", "deleted": n})
}

// handleDeleteSessions removes selected sessions and all operations/snapshots
// belonging to them.
func (s *Server) handleDeleteSessions(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionIDs []string `json:"session_ids"`
		IDs        []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, http.StatusBadRequest, 2, "invalid JSON body: "+err.Error(), "")
		return
	}
	ids := body.SessionIDs
	if len(ids) == 0 {
		ids = body.IDs
	}
	if len(ids) == 0 {
		writeAPIError(w, http.StatusBadRequest, 2, "session_ids is required", "select at least one session")
		return
	}
	n, err := s.app.Audit.DeleteSessions(ids)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "deleted", "deleted": n})
}

// handleAuditStream pushes new/changed ops via Server-Sent Events.
func (s *Server) handleAuditStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, 5, "streaming not supported", "")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Establish connection with a comment
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	// Track seen ops: opID -> status
	seen := make(map[string]string)

	// Send initial batch of recent ops
	if ops, err := s.app.ListOps("", "", 0); err == nil && len(ops) > 0 {
		for _, op := range ops {
			seen[op.OpID] = op.Status
		}
		s.writeSSEBatch(w, ops)
		flusher.Flush()
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ops, err := s.app.ListOps("", "", 0)
			if err != nil {
				continue
			}
			var changed []*audit.Op
			for _, op := range ops {
				prev, exists := seen[op.OpID]
				if !exists || prev != op.Status {
					changed = append(changed, op)
					seen[op.OpID] = op.Status
				}
			}
			if len(changed) > 0 {
				if s.writeSSEBatch(w, changed) {
					flusher.Flush()
				} else {
					return // write failed, client gone
				}
			}
		}
	}
}

// writeSSEBatch writes ops as an SSE data event. Returns false on write error.
func (s *Server) writeSSEBatch(w http.ResponseWriter, ops []*audit.Op) bool {
	data, err := json.Marshal(map[string]interface{}{"ops": ops})
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return false
	}
	return true
}

// =============================================================================
// Static files / SPA
// =============================================================================

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	// Return 404 JSON for unmatched API paths
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeAPIError(w, http.StatusNotFound, 5, "endpoint not found: "+r.URL.Path, "")
		return
	}

	// Try to serve the file from embedded dist
	if r.URL.Path != "/" {
		rel := strings.TrimPrefix(r.URL.Path, "/")
		if info, err := fs.Stat(s.distRoot, rel); err == nil && !info.IsDir() {
			s.fileServer.ServeHTTP(w, r)
			return
		}
	}

	// SPA fallback: serve index.html
	r.URL.Path = "/"
	s.fileServer.ServeHTTP(w, r)
}
