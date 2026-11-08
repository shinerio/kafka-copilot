// API client for Kafka Pilot backend (served by `kp serve`).
// All calls go through the Vite dev proxy (/api -> 127.0.0.1:18080) or,
// in production, the embedded Go server that serves the same paths.

import type {
  ApiError,
  BatchResult,
  KeyPartitionInfo,
  LagInfo,
  Op,
  ProduceBatchRequestBody,
  ProduceRequestBody,
  Profile,
  ProfileInput,
  ResultMessage,
  ScanRequestBody,
  ScanResult,
  Session,
  Snapshot,
  TopicInfo,
} from './types'

const API_BASE = '/api'

export class HttpError extends Error {
  status: number
  body: ApiError
  constructor(status: number, body: ApiError) {
    super(body?.message || `HTTP ${status}`)
    this.status = status
    this.body = body
  }
}

async function request<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    headers: {
      Accept: 'application/json',
      ...(init?.body != null && !(init.body instanceof FormData)
        ? { 'Content-Type': 'application/json' }
        : {}),
      ...init?.headers,
    },
    ...init,
  })

  const text = await resp.text()
  let parsed: unknown = null
  if (text) {
    try {
      parsed = JSON.parse(text)
    } catch {
      parsed = text
    }
  }

  if (!resp.ok) {
    const errBody: ApiError =
      typeof parsed === 'object' && parsed !== null
        ? (parsed as ApiError)
        : { code: resp.status, message: typeof parsed === 'string' ? parsed : resp.statusText }
    throw new HttpError(resp.status, errBody)
  }

  return parsed as T
}

// --- Profiles ---

export const api = {
  // Profiles
  listProfiles: () => request<Profile[]>('/profiles'),
  createProfile: (body: ProfileInput) =>
    request<Profile>('/profiles', { method: 'POST', body: JSON.stringify(body) }),
  updateProfile: (name: string, body: ProfileInput) =>
    request<Profile>(`/profiles/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify(body),
    }),
  deleteProfile: (name: string) =>
    request<{ status: string }>(`/profiles/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    }),
  setDefaultProfile: (name: string) =>
    request<{ status: string }>(`/profiles/${encodeURIComponent(name)}/default`, {
      method: 'PUT',
    }),

  // Topics
  listTopics: (filter?: string) =>
    request<TopicInfo[]>(
      `/topics${filter ? `?filter=${encodeURIComponent(filter)}` : ''}`,
    ),
  describeTopic: (topic: string) =>
    request<TopicInfo>(`/topics/${encodeURIComponent(topic)}`),

  // Scan
  // Read-only: which partition would the standard Kafka partitioner pick?
  keyPartition: (topic: string, key: string) =>
    request<KeyPartitionInfo>(
      `/key-partition?topic=${encodeURIComponent(topic)}&key=${encodeURIComponent(key)}`,
    ),
  scan: (body: ScanRequestBody) =>
    request<ScanResult>('/scan', { method: 'POST', body: JSON.stringify(body) }),

  // Produce
  produceSingle: (body: ProduceRequestBody) =>
    request<{ topic: string; partition: number; offset: number; produced: number }>(
      '/produce',
      { method: 'POST', body: JSON.stringify(body) },
    ),
  produceBatch: (body: ProduceBatchRequestBody) =>
    request<BatchResult>('/produce/batch', {
      method: 'POST',
      body: JSON.stringify(body),
    }),

  // Groups
  listGroups: () => request<string[]>('/groups'),
  groupLag: (group: string, topic?: string) =>
    request<LagInfo>(
      `/groups/${encodeURIComponent(group)}/lag${topic ? `?topic=${encodeURIComponent(topic)}` : ''}`,
    ),

  // Sessions
  listSessions: () => request<Session[]>('/sessions'),
  getSession: (id: string) => request<Session>(`/sessions/${encodeURIComponent(id)}`),

  // Audit
  listOps: (params?: { session?: string; source?: string; since?: string }) => {
    const qs = new URLSearchParams()
    if (params?.session) qs.set('session', params.session)
    if (params?.source) qs.set('source', params.source)
    if (params?.since) qs.set('since', params.since)
    const q = qs.toString()
    return request<Op[]>(`/audit/ops${q ? `?${q}` : ''}`)
  },
  getOp: (opId: string) =>
    request<Op>(`/audit/ops/${encodeURIComponent(opId)}`),
  getSnapshot: (opId: string) =>
    request<Snapshot>(`/audit/ops/${encodeURIComponent(opId)}/snapshot`),
  clearAudit: (before?: string) => {
    const qs = before ? `?before=${encodeURIComponent(before)}` : ''
    return request<{ status: string }>(`/audit${qs}`, { method: 'DELETE' })
  },
  // Observation page: delete explicitly selected records.
  deleteOps: (ids: string[]) =>
    request<{ status: string; deleted: number }>('/audit/ops', {
      method: 'DELETE',
      body: JSON.stringify({ op_ids: ids }),
    }),
  deleteSessions: (ids: string[]) =>
    request<{ status: string; deleted: number }>('/sessions', {
      method: 'DELETE',
      body: JSON.stringify({ session_ids: ids }),
    }),

  // Health (used for connection-status indicator in the top bar)
  health: () => request<{ status: string }>('/health'),

  // SSE: /api/audit/stream. Returns the EventSource so callers can close it.
  auditStream: (handlers: {
    onEvent?: (ev: MessageEvent) => void
    onError?: (e: Event) => void
    onOpen?: () => void
  }) => {
    const es = new EventSource(`${API_BASE}/audit/stream`)
    es.onmessage = (ev) => handlers.onEvent?.(ev)
    es.addEventListener('op.started', (ev) => handlers.onEvent?.(ev as MessageEvent))
    es.addEventListener('op.progress', (ev) => handlers.onEvent?.(ev as MessageEvent))
    es.addEventListener('op.finished', (ev) => handlers.onEvent?.(ev as MessageEvent))
    es.addEventListener('session', (ev) => handlers.onEvent?.(ev as MessageEvent))
    es.addEventListener('heartbeat', (ev) => handlers.onEvent?.(ev as MessageEvent))
    es.onopen = () => handlers.onOpen?.()
    es.onerror = (e) => handlers.onError?.(e)
    return es
  },
}

// Helpers to normalize the dual snake/camel JSON keys the Go tags produce.
export function opId(op: Op): string {
  return op.op_id || op.opId || ''
}
export function opStarted(op: Op): number {
  return op.started_at || op.startedAt || 0
}
export function opDuration(op: Op): number {
  return op.duration_ms || op.durationMs || 0
}
export function sessionId(s: Session): string {
  return s.session_id || s.sessionId || ''
}
export function sessionStarted(s: Session): number {
  return s.started_at || s.startedAt || 0
}
export function sessionEnded(s: Session): number {
  return s.ended_at || s.endedAt || 0
}
export function snapshotMessages(s: Snapshot): ResultMessage[] {
  if (Array.isArray(s.messages)) return s.messages as ResultMessage[]
  try {
    return JSON.parse(s.messages as string) as ResultMessage[]
  } catch {
    return []
  }
}
