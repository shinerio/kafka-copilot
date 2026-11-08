package produce

// BatchRequest holds script-based batch produce parameters.
type BatchRequest struct {
	Topic       string
	ScriptPath  string
	Count       int
	Concurrency int
	Rate        int    // msgs/sec, 0 = unlimited
	DryRun      bool
	QueueCap    int    // bounded queue for backpressure (default 1024)
}

// BatchResult holds batch produce results.
type BatchResult struct {
	Produced  int64    `json:"produced"`
	Failed    int64    `json:"failed"`
	Cancelled bool     `json:"cancelled"`
	TPS       float64  `json:"tps"`
	ElapsedMs int64    `json:"elapsedMs"`
	DryRun    bool     `json:"dryRun,omitempty"`
	Samples   []Sample `json:"samples,omitempty"`
}

// Sample is a preview message for dry-run.
type Sample struct {
	Key       string            `json:"key,omitempty"`
	Value     any               `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Partition int32             `json:"partition,omitempty"`
}
