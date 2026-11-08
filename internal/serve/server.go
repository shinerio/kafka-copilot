package serve

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
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
		app:  a,
		mux:  http.NewServeMux(),
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
	s.mux.HandleFunc("POST /api/scan", s.handleScan)
	s.mux.HandleFunc("POST /api/produce", s.handleProduce)
	s.mux.HandleFunc("POST /api/produce/batch", s.handleProduceBatch)

	// Groups
	s.mux.HandleFunc("GET /api/groups", s.handleListGroups)
	s.mux.HandleFunc("GET /api/groups/{group}/lag", s.handleGroupLag)

	// Sessions
	s.mux.HandleFunc("GET /api/sessions", s.handleListSessions)
	s.mux.HandleFunc("GET /api/sessions/{id}", s.handleGetSession)

	// Audit
	s.mux.HandleFunc("GET /api/audit/ops", s.handleListOps)
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

// parseTimeParam accepts Unix millis (int) or RFC3339 string.
func parseTimeParam(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("invalid time (use Unix millis or RFC3339): %w", err)
	}
	return t.UnixMilli(), nil
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
// Scan
// =============================================================================

type scanRequest struct {
	Profile    string  `json:"profile,omitempty"`
	Topic      string  `json:"topic"`
	Last       int     `json:"last,omitempty"`
	Since      string  `json:"since,omitempty"`
	Until      string  `json:"until,omitempty"`
	Key        string  `json:"key,omitempty"`
	KeyRegex   string  `json:"key_regex,omitempty"`
	Keyword    string  `json:"keyword,omitempty"`
	Expr       string  `json:"expr,omitempty"`
	Partitions []int32 `json:"partitions,omitempty"`
	Timeout    int     `json:"timeout,omitempty"`
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
		Profile:    body.Profile,
		Topic:      body.Topic,
		Last:       body.Last,
		Key:        body.Key,
		KeyRegex:   body.KeyRegex,
		Keyword:    body.Keyword,
		Expr:       body.Expr,
		Partitions: body.Partitions,
		TimeoutSec: body.Timeout,
		IgnoreCase: true,
	}

	if body.Since != "" {
		t, err := time.Parse(time.RFC3339, body.Since)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, 2, "invalid since format (use RFC3339): "+err.Error(), "")
			return
		}
		req.Since = t
		req.HasTimeWindow = true
	}
	if body.Until != "" {
		t, err := time.Parse(time.RFC3339, body.Until)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, 2, "invalid until format (use RFC3339): "+err.Error(), "")
			return
		}
		req.Until = t
		req.HasTimeWindow = true
	}

	if req.Last == 0 && !req.HasTimeWindow {
		req.Last = 100 // default backscan
	}

	// Create audit op (source=human)
	command := fmt.Sprintf("HTTP scan topic=%s", body.Topic)
	if body.Key != "" {
		command += " key=" + body.Key
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
	Profile   string   `json:"profile,omitempty"`
	Topic     string   `json:"topic"`
	Key       string   `json:"key,omitempty"`
	Value     string   `json:"value"`
	Headers   []string `json:"headers,omitempty"`
	Partition *int32   `json:"partition,omitempty"`
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

	partition := int32(-1) // auto
	if body.Partition != nil {
		partition = *body.Partition
	}

	command := fmt.Sprintf("HTTP produce topic=%s", body.Topic)
	opCtx := s.app.NewOpContext("", "human", app.GetActor(), "", command)

	result, err := s.app.ProduceSingle(r.Context(), opCtx, body.Profile, body.Topic,
		[]byte(body.Key), []byte(body.Value), body.Headers, partition)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

type batchProduceRequest struct {
	Profile     string `json:"profile,omitempty"`
	Topic       string `json:"topic"`
	Script      string `json:"script"`
	Count       int    `json:"count"`
	Concurrency int    `json:"concurrency,omitempty"`
	Rate        int    `json:"rate,omitempty"`
	DryRun      bool   `json:"dry_run,omitempty"`
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
	if body.Script == "" {
		writeAPIError(w, http.StatusBadRequest, 2, "script is required", "")
		return
	}

	req := &produce.BatchRequest{
		Topic:       body.Topic,
		ScriptPath:  body.Script,
		Count:       body.Count,
		Concurrency: body.Concurrency,
		Rate:        body.Rate,
		DryRun:      body.DryRun,
	}
	if req.Concurrency == 0 {
		req.Concurrency = 8
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
