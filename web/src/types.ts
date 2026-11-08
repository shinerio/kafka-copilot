// TypeScript types mirroring the Kafka Pilot Go structs (JSON tags).
// Kept in sync with internal/config, internal/kafka, internal/filter,
// internal/audit, internal/produce and internal/app.

// --- Profiles (internal/config) ---

export interface SASL {
  mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512' | string
  username: string
  password: string // masked as "****" on read; plaintext on write
}

export interface TLS {
  ca_cert?: string
  caCert?: string
  skip_verify?: boolean
  skipVerify?: boolean
}

export interface Profile {
  name: string
  bootstrap_servers: string[]
  bootstrapServers?: string[]
  sasl?: SASL | null
  tls?: TLS | null
}

// Payloads for POST/PUT /api/profiles
export interface ProfileInput {
  name: string
  bootstrap_servers: string[]
  sasl?: SASL | null
  tls?: TLS | null
}

// --- Topics (internal/kafka) ---

export interface PartitionInfo {
  partition: number
  latest_offset: number
}

export interface TopicInfo {
  name: string
  partitions_count: number
  partitions: PartitionInfo[]
}

// --- Groups / Lag (internal/kafka) ---

export interface ConsumerInfo {
  client_id: string
  clientID?: string
  member_id: string
  memberID?: string
  client_host: string
  clientHost?: string
  assignments: string[]
}

export interface GroupInfo {
  name: string
  state: string
  consumers?: ConsumerInfo[]
}

export interface PartitionLag {
  partition: number
  committed: number
  hwm: number
  lag: number
}

export interface LagInfo {
  topic: string
  partitions: PartitionLag[]
  total_lag: number
  totalLag?: number
  high_lag: boolean
  highLag?: boolean
}

// --- Scan (internal/app + internal/filter) ---

export interface ScanRequestBody {
  topic: string
  last?: number
  since?: string // RFC3339
  until?: string // RFC3339
  key?: string
  keyword?: string
  expr?: string
  partitions?: number[]
}

export interface ResultMessage {
  key: string
  value: unknown
  truncated?: boolean
  headers?: Record<string, string>
  partition: number
  offset: number
  timestamp: string
  size: number
}

export interface ScanResult {
  opId: string
  topic: string
  total: number
  matched: number
  truncated: boolean
  warnings?: string[]
  elapsedMs: number
  messages?: ResultMessage[]
  skippedNonUtf8Key?: number
}

// --- Produce (internal/produce) ---

export interface ProduceRequestBody {
  topic: string
  key?: string
  value: string
  headers?: Record<string, string>
  partition?: number
}

export interface ProduceBatchRequestBody {
  topic: string
  script: string
  count: number
  concurrency: number
  rate: number
  dry_run: boolean
}

export interface Sample {
  key?: string
  value: unknown
  headers?: Record<string, string>
  partition?: number
}

export interface BatchResult {
  produced: number
  failed: number
  cancelled: boolean
  tps: number
  elapsedMs: number
  dryRun?: boolean
  samples?: Sample[]
}

// --- Audit (internal/audit) ---

export interface Op {
  op_id: string
  opId?: string
  session_id?: string
  sessionId?: string
  source: string // ai | human
  actor: string
  intent?: string
  command: string
  commandTruncated?: boolean
  started_at: number
  startedAt?: number
  duration_ms: number
  durationMs?: number
  status: string // running|ok|error|truncated|cancelled|crashed
  result_summary?: string
  resultSummary?: string
  warnings?: string
  error?: string
}

export interface Session {
  session_id: string
  sessionId?: string
  label?: string
  started_at: number
  startedAt?: number
  ended_at?: number
  endedAt?: number
  op_count: number
  opCount?: number
  auto?: boolean
}

export interface Snapshot {
  op_id: string
  opId?: string
  session_id?: string
  sessionId?: string
  created_at: number
  createdAt?: number
  messages: ResultMessage[] | string
  truncated: boolean
}

// SSE event on /api/audit/stream
export interface AuditStreamEvent {
  event: 'op.started' | 'op.progress' | 'op.finished' | 'session' | 'heartbeat'
  data: Op | { opId: string; produced?: number; failed?: number }
}

// Generic API error envelope: { code, message, hint? }
export interface ApiError {
  code: number | string
  message: string
  hint?: string
}
