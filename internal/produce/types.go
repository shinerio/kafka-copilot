package produce

// BatchRequest holds script-based batch produce parameters.
// A script can be supplied either as inline source (Web UI uploads file
// content, since a browser path is meaningless on the server) or as a path
// to a local .js file (CLI). ScriptContent takes precedence when both are set.
type BatchRequest struct {
	Topic         string
	ScriptPath    string // CLI: path to a .js file on the local filesystem
	ScriptContent string // Web UI: inline JS source
	Count         int
	Concurrency   int
	Rate          int  // msgs/sec, 0 = unlimited
	DryRun        bool
	QueueCap      int  // bounded queue for backpressure (default 1024)
}

// BatchResult holds batch produce results.
// GenerateCalls / MessagesBuilt expose the generation model to the user:
// how many times generate(ctx) ran, and how many records it actually built.
type BatchResult struct {
	Produced      int64    `json:"produced"`
	Failed        int64    `json:"failed"`
	Cancelled     bool     `json:"cancelled"`
	TPS           float64  `json:"tps"`
	ElapsedMs     int64    `json:"elapsedMs"`
	GenerateCalls int64    `json:"generateCalls"`
	MessagesBuilt int64    `json:"messagesBuilt"`
	DryRun        bool     `json:"dryRun,omitempty"`
	Samples       []Sample `json:"samples,omitempty"`
}

// Sample is a preview message for dry-run.
type Sample struct {
	Key       string            `json:"key,omitempty"`
	Value     any               `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Partition int32             `json:"partition,omitempty"`
}
