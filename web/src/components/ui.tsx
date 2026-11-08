// Shared UI primitives built on Fluent UI v9.
// These wrap common patterns (status pills, JSON viewer, pagination,
// progress bar, empty/loading/error states) so pages stay compact.

import {
  Button,
  Spinner as FluentSpinner,
  MessageBar,
  MessageBarBody,
  MessageBarTitle,
  Subtitle2,
  Body1,
  makeStyles,
  tokens,
  mergeClasses,
} from '@fluentui/react-components'
import {
  ChevronLeftRegular,
  ChevronRightRegular,
  ErrorCircleRegular,
  InfoRegular,
} from '@fluentui/react-icons'
import type { ReactNode } from 'react'
import { useMemo } from 'react'

const useStyles = makeStyles({
  empty: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '8px',
    padding: '40px 16px',
    color: tokens.colorNeutralForeground3,
    textAlign: 'center',
  },
  pager: {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    justifyContent: 'flex-end',
    paddingTop: '8px',
  },
  statStrip: {
    display: 'flex',
    gap: '24px',
    alignItems: 'center',
    flexWrap: 'wrap',
  },
  stat: {
    display: 'flex',
    flexDirection: 'column',
    gap: '2px',
  },
  statLabel: {
    fontSize: '12px',
    color: tokens.colorNeutralForeground3,
  },
  statValue: {
    fontSize: '18px',
    fontWeight: 600,
    fontVariantNumeric: 'tabular-nums',
  },
})

// --- Status pill ---

export type StatusKind = 'ok' | 'running' | 'error' | 'warning' | 'muted' | 'cancelled' | 'truncated'

const STATUS_PILL_CLASS: Record<StatusKind, string> = {
  ok: 'kp-pill kp-pill--ok',
  running: 'kp-pill kp-pill--running',
  error: 'kp-pill kp-pill--error',
  warning: 'kp-pill kp-pill--warning',
  muted: 'kp-pill kp-pill--muted',
  cancelled: 'kp-pill kp-pill--muted',
  truncated: 'kp-pill kp-pill--warning',
}

const STATUS_COLOR: Record<StatusKind, string> = {
  ok: '#107c10',
  running: '#0f6cbd',
  error: '#c4314b',
  warning: '#9b5300',
  muted: '#8a8a8a',
  cancelled: '#8a8a8a',
  truncated: '#9b5300',
}

export function StatusPill({ status, label }: { status: StatusKind; label?: string }) {
  return (
    <span
      className={STATUS_PILL_CLASS[status]}
      style={{ minWidth: 64, justifyContent: 'center', display: 'inline-flex' }}
    >
      {label || status}
    </span>
  )
}

export function statusKind(s: string): StatusKind {
  switch (s) {
    case 'ok':
      return 'ok'
    case 'running':
      return 'running'
    case 'error':
    case 'crashed':
      return 'error'
    case 'warning':
      return 'warning'
    case 'cancelled':
      return 'cancelled'
    case 'truncated':
      return 'truncated'
    default:
      return 'muted'
  }
}

// --- JSON viewer with naive syntax highlighting ---

function highlight(json: string): string {
  // Escape HTML first
  const esc = json
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
  return esc.replace(
    /("(\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g,
    (match) => {
      let cls = 'kp-json__number'
      if (/^"/.test(match)) {
        cls = /:$/.test(match) ? 'kp-json__key' : 'kp-json__string'
      } else if (/true|false/.test(match)) {
        cls = 'kp-json__bool'
      } else if (/null/.test(match)) {
        cls = 'kp-json__null'
      }
      return `<span class="${cls}">${match}</span>`
    },
  )
}

export function JsonView({ value, maxHeight }: { value: unknown; maxHeight?: number }) {
  const html = useMemo(() => {
    try {
      return highlight(JSON.stringify(value, null, 2))
    } catch {
      return highlight(String(value))
    }
  }, [value])
  return (
    <pre
      className="kp-json"
      style={maxHeight ? { maxHeight } : undefined}
      dangerouslySetInnerHTML={{ __html: html }}
    />
  )
}

// --- Progress bar (div + CSS animation) ---

export function ProgressBar({
  value,
  indeterminate,
  label,
}: {
  value?: number // 0..1
  indeterminate?: boolean
  label?: string
}) {
  const pct = Math.max(0, Math.min(1, value ?? 0)) * 100
  return (
    <div>
      {label && (
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
          <Body1>{label}</Body1>
          {!indeterminate && value != null && (
            <Body1 className="kp-cell-numeric">{Math.round(pct)}%</Body1>
          )}
        </div>
      )}
      <div className="kp-progress">
        <div
          className={mergeClasses('kp-progress__bar', indeterminate && 'kp-progress__bar--indeterminate')}
          style={indeterminate ? undefined : { width: `${pct}%` }}
        />
      </div>
    </div>
  )
}

// --- Pagination ---

export function Pagination({
  page,
  pageSize,
  total,
  onPage,
}: {
  page: number
  pageSize: number
  total: number
  onPage: (p: number) => void
}) {
  const styles = useStyles()
  const pages = Math.max(1, Math.ceil(total / pageSize))
  const from = total === 0 ? 0 : (page - 1) * pageSize + 1
  const to = Math.min(total, page * pageSize)
  return (
    <div className={styles.pager}>
      <Body1 className="kp-muted">
        {from}-{to} / {total}
      </Body1>
      <Button
        size="small"
        icon={<ChevronLeftRegular />}
        disabled={page <= 1}
        onClick={() => onPage(page - 1)}
      />
      <Body1>
        {page} / {pages}
      </Body1>
      <Button
        size="small"
        icon={<ChevronRightRegular />}
        disabled={page >= pages}
        onClick={() => onPage(page + 1)}
      />
    </div>
  )
}

// --- Empty / Loading / Error states ---

export function EmptyState({ title, hint }: { title: string; hint?: string }) {
  const styles = useStyles()
  return (
    <div className={styles.empty}>
      <InfoRegular fontSize={28} />
      <Subtitle2>{title}</Subtitle2>
      {hint && <Body1>{hint}</Body1>}
    </div>
  )
}

export function LoadingRow() {
  return (
    <div className="kp-row" style={{ padding: 16, justifyContent: 'center' }}>
      <FluentSpinner size="tiny" />
      <Body1 className="kp-muted">加载中…</Body1>
    </div>
  )
}

export function ErrorState({ error }: { error: unknown }) {
  const msg = error instanceof Error ? error.message : String(error)
  return (
    <MessageBar intent="error">
      <MessageBarBody>
        <MessageBarTitle>请求失败</MessageBarTitle>
        {msg}
      </MessageBarBody>
    </MessageBar>
  )
}

// --- Stat strip (total / matched / elapsed) ---

export function StatStrip({
  stats,
}: {
  stats: { label: string; value: ReactNode; tone?: StatusKind }[]
}) {
  const styles = useStyles()
  return (
    <div className={styles.statStrip}>
      {stats.map((s) => (
        <div key={s.label} className={styles.stat}>
          <span className={styles.statLabel}>{s.label}</span>
          <span
            className={styles.statValue}
            style={s.tone ? { color: STATUS_COLOR[s.tone] } : undefined}
          >
            {s.value}
          </span>
        </div>
      ))}
    </div>
  )
}

// Re-export ErrorCircleRegular for pages that want a danger icon.
export { ErrorCircleRegular }
