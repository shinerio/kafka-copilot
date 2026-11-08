// Message scan page (FR-04/05): build a scan query, show total/matched/elapsed
// stats, then render matched messages in a paginated table with row expansion
// to a JSON view of key/value/headers.

import {
  Title3,
  Body1,
  Subtitle2,
  Input,
  Field,
  Button,
  SpinButton,
  Table,
  TableHeader,
  TableHeaderCell,
  TableBody,
  TableRow,
  TableCell,
  Card,
  makeStyles,
  tokens,
  MessageBar,
  MessageBarBody,
} from '@fluentui/react-components'
import { Search24Regular, ChevronRightRegular, ChevronDownRegular } from '@fluentui/react-icons'
import { Fragment, useCallback, useMemo, useState } from 'react'
import { api } from '../api'
import type { ResultMessage, ScanResult } from '../types'
import { EmptyState, ErrorState, JsonView, Pagination, StatStrip, statusKind } from '../components/ui'

const PAGE_SIZE = 50

const useStyles = makeStyles({
  formGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
    gap: '12px',
  },
  toolbar: {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    marginTop: '12px',
  },
  tableWrap: {
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: '8px',
    overflow: 'hidden',
    background: tokens.colorNeutralBackground1,
  },
  jsonCell: {
    background: tokens.colorNeutralBackground2,
  },
})

function fmtTs(ts: string): string {
  if (!ts) return ''
  const d = new Date(ts)
  return isNaN(d.getTime()) ? ts : d.toLocaleString()
}

function valuePreview(v: unknown): string {
  if (v == null) return ''
  if (typeof v === 'string') return v
  try {
    return JSON.stringify(v)
  } catch {
    return String(v)
  }
}

export function ScanPage({ profile }: { profile: string }) {
  const styles = useStyles()
  const [topic, setTopic] = useState('')
  const [last, setLast] = useState(100)
  const [since, setSince] = useState('')
  const [until, setUntil] = useState('')
  const [key, setKey] = useState('')
  const [keyword, setKeyword] = useState('')
  const [expr, setExpr] = useState('')
  const [partitions, setPartitions] = useState('')
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<ScanResult | null>(null)
  const [error, setError] = useState<unknown>(null)
  const [page, setPage] = useState(1)
  const [expandedRow, setExpandedRow] = useState<number | null>(null)

  const messages = useMemo<ResultMessage[]>(() => result?.messages || [], [result])

  const run = useCallback(async () => {
    if (!topic) {
      setError(new Error('topic 必填'))
      return
    }
    setRunning(true)
    setError(null)
    setResult(null)
    setPage(1)
    setExpandedRow(null)
    try {
      const parts = partitions
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s !== '')
        .map((s) => Number(s))
        .filter((n) => !isNaN(n))
      const res = await api.scan({
        topic,
        last,
        since: since || undefined,
        until: until || undefined,
        key: key || undefined,
        keyword: keyword || undefined,
        expr: expr || undefined,
        partitions: parts.length ? parts : undefined,
      })
      setResult(res)
    } catch (e) {
      setError(e)
    } finally {
      setRunning(false)
    }
  }, [topic, last, since, until, key, keyword, expr, partitions])

  const totalPages = Math.max(1, Math.ceil(messages.length / PAGE_SIZE))
  const pageRows = useMemo(
    () => messages.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE),
    [messages, page],
  )

  return (
    <div className="kp-fill" style={{ gap: 12 }}>
      <div className="kp-row">
        <Title3>消息查询</Title3>
        {profile && <Body1 className="kp-muted">profile: <b>{profile}</b></Body1>}
        <div className="kp-grow" />
      </div>

      <Card className="kp-card">
        <Subtitle2>查询条件</Subtitle2>
        <div className={styles.formGrid} style={{ marginTop: 8 }}>
          <Field label="Topic *">
            <Input
              contentBefore={<Search24Regular />}
              placeholder="topic 名称"
              value={topic}
              onChange={(_, d) => setTopic(d.value)}
            />
          </Field>
          <Field label="回扫量 (--last)">
            <SpinButton
              value={last}
              min={1}
              max={5000}
              onChange={(_, d) => setLast(Number(d.value) || 0)}
            />
          </Field>
          <Field label="分区 (逗号分隔, 可选)">
            <Input
              placeholder="0,2,3"
              value={partitions}
              onChange={(_, d) => setPartitions(d.value)}
            />
          </Field>
          <Field label="Since (RFC3339)">
            <Input
              placeholder="2026-01-01T00:00:00Z"
              value={since}
              onChange={(_, d) => setSince(d.value)}
            />
          </Field>
          <Field label="Until (RFC3339)">
            <Input
              placeholder="2026-01-02T00:00:00Z"
              value={until}
              onChange={(_, d) => setUntil(d.value)}
            />
          </Field>
          <Field label="--key (精确)">
            <Input value={key} onChange={(_, d) => setKey(d.value)} />
          </Field>
          <Field label="--keyword (子串)">
            <Input value={keyword} onChange={(_, d) => setKeyword(d.value)} />
          </Field>
          <Field label="--expr (jq)">
            <Input value={expr} onChange={(_, d) => setExpr(d.value)} placeholder=".field == 1" />
          </Field>
        </div>
        <div className={styles.toolbar}>
          <Button appearance="primary" onClick={run} disabled={running || !topic}>
            {running ? '扫描中…' : '扫描'}
          </Button>
          {running && <span className="kp-spinner" />}
          {(since || until) && (
            <Body1 className="kp-muted">时间窗口已启用</Body1>
          )}
        </div>
      </Card>

      {Boolean(error) && <ErrorState error={error} />}

      {result && (
        <>
          <Card className="kp-card">
            <div className="kp-row" style={{ justifyContent: 'space-between' }}>
              <StatStrip
                stats={[
                  { label: 'Total', value: result.total.toLocaleString() },
                  {
                    label: 'Matched',
                    value: result.matched.toLocaleString(),
                    tone: statusKind(result.truncated ? 'truncated' : 'ok'),
                  },
                  { label: 'Elapsed (ms)', value: result.elapsedMs.toLocaleString() },
                  { label: 'Op ID', value: <span className="kp-mono">{result.opId}</span> },
                ]}
              />
              {result.truncated && (
                <Body1 className="kp-muted">结果已截断</Body1>
              )}
            </div>
            {result.warnings && result.warnings.length > 0 && (
              <MessageBar intent="warning" style={{ marginTop: 8 }}>
                <MessageBarBody>{result.warnings.join('; ')}</MessageBarBody>
              </MessageBar>
            )}
          </Card>

          <div className={styles.tableWrap}>
            <Table size="small">
              <TableHeader>
                <TableRow>
                  <TableHeaderCell style={{ width: 32 }} />
                  <TableHeaderCell>Key</TableHeaderCell>
                  <TableHeaderCell>Value</TableHeaderCell>
                  <TableHeaderCell className="kp-cell-numeric">Partition</TableHeaderCell>
                  <TableHeaderCell className="kp-cell-numeric">Offset</TableHeaderCell>
                  <TableHeaderCell>Timestamp</TableHeaderCell>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pageRows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6}>
                      <EmptyState title="无匹配消息" />
                    </TableCell>
                  </TableRow>
                )}
                {pageRows.map((m, i) => {
                  const idx = (page - 1) * PAGE_SIZE + i
                  const isOpen = expandedRow === idx
                  return (
                    <Fragment key={idx}>
                      <TableRow
                        style={{ cursor: 'pointer' }}
                        onClick={() => setExpandedRow(isOpen ? null : idx)}
                      >
                        <TableCell>
                          {isOpen ? <ChevronDownRegular /> : <ChevronRightRegular />}
                        </TableCell>
                        <TableCell className="kp-cell-mono kp-truncate" style={{ maxWidth: 200 }}>
                          {m.key || '(空)'}
                        </TableCell>
                        <TableCell className="kp-cell-mono kp-truncate" style={{ maxWidth: 320 }}>
                          {valuePreview(m.value)}
                        </TableCell>
                        <TableCell className="kp-cell-numeric">{m.partition}</TableCell>
                        <TableCell className="kp-cell-numeric">{m.offset.toLocaleString()}</TableCell>
                        <TableCell>{fmtTs(m.timestamp)}</TableCell>
                      </TableRow>
                      {isOpen && (
                        <TableRow className={styles.jsonCell}>
                          <TableCell />
                          <TableCell colSpan={5}>
                            <JsonView
                              value={{
                                key: m.key,
                                value: m.value,
                                headers: m.headers,
                                partition: m.partition,
                                offset: m.offset,
                                timestamp: m.timestamp,
                                size: m.size,
                                truncated: m.truncated,
                              }}
                            />
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  )
                })}
              </TableBody>
            </Table>
          </div>

          <Pagination
            page={page}
            pageSize={PAGE_SIZE}
            total={messages.length}
            onPage={setPage}
          />
          {page > totalPages && setPage(1)}
          <Body1 className="kp-muted" style={{ fontSize: 12 }}>
            共 {messages.length.toLocaleString()} 条命中，每页 {PAGE_SIZE} 条
          </Body1>
        </>
      )}
    </div>
  )
}
