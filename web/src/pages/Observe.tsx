// AI observation page (FR-09/10): session list -> op timeline -> op detail
// (command, status, result stats) with a snapshot replay button.

import {
  Title3,
  Body1,
  Subtitle2,
  Button,
  Table,
  TableHeader,
  TableHeaderCell,
  TableBody,
  TableRow,
  TableCell,
  Card,
  Divider,
  makeStyles,
  tokens,
} from '@fluentui/react-components'
import {
  ArrowClockwiseRegular,
  HistoryRegular,
  PlayCircleRegular,
  EyeRegular,
} from '@fluentui/react-icons'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { api, opDuration, opId, opStarted, sessionId, sessionEnded, sessionStarted, snapshotMessages } from '../api'
import type { Op, Session, Snapshot, ResultMessage } from '../types'
import { EmptyState, ErrorState, JsonView, LoadingRow, StatusPill, statusKind } from '../components/ui'

const useStyles = makeStyles({
  shell: {
    display: 'grid',
    gridTemplateColumns: 'minmax(200px, 1fr) minmax(0, 2fr)',
    gap: '12px',
    alignItems: 'start',
  },
  detailSplit: {
    display: 'grid',
    gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)',
    gap: '12px',
  },
  listWrap: {
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: '8px',
    overflow: 'auto',
    background: tokens.colorNeutralBackground1,
    maxHeight: '72vh',
  },
  listItem: {
    padding: '8px 12px',
    cursor: 'pointer',
    borderBottom: `1px solid ${tokens.colorNeutralStroke2}`,
    '&:hover': { background: tokens.colorNeutralBackground1Hover },
  },
  listItemActive: {
    background: tokens.colorBrandBackground2,
    color: tokens.colorBrandForeground1,
  },
  tableWrap: {
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: '8px',
    overflow: 'hidden',
    background: tokens.colorNeutralBackground1,
  },
})

function fmtTime(ms: number): string {
  if (!ms) return ''
  return new Date(ms).toLocaleString()
}

function parseSummary(s: string | undefined): unknown {
  if (!s) return null
  try {
    return JSON.parse(s)
  } catch {
    return s
  }
}

export function ObservePage() {
  const styles = useStyles()
  const [sessions, setSessions] = useState<Session[]>([])
  const [selectedSession, setSelectedSession] = useState<string | null>(null)
  const [ops, setOps] = useState<Op[]>([])
  const [selectedOp, setSelectedOp] = useState<Op | null>(null)
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [snapLoading, setSnapLoading] = useState(false)
  const [loading, setLoading] = useState(false)
  const [opsLoading, setOpsLoading] = useState(false)
  const [error, setError] = useState<unknown>(null)

  const loadSessions = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const list = await api.listSessions()
      setSessions(list || [])
    } catch (e) {
      setError(e)
      setSessions([])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadSessions()
  }, [loadSessions])

  const loadOps = useCallback(async () => {
    if (!selectedSession) return
    setOpsLoading(true)
    setSelectedOp(null)
    setSnapshot(null)
    try {
      const list = await api.listOps({ session: selectedSession })
      setOps(list || [])
    } catch {
      setOps([])
    } finally {
      setOpsLoading(false)
    }
  }, [selectedSession])

  useEffect(() => {
    if (selectedSession) void loadOps()
  }, [selectedSession, loadOps])

  const loadSnapshot = useCallback(async (op: Op) => {
    const id = opId(op)
    setSnapLoading(true)
    setSnapshot(null)
    try {
      const snap = await api.getSnapshot(id)
      setSnapshot(snap)
    } catch {
      setSnapshot(null)
    } finally {
      setSnapLoading(false)
    }
  }, [])

  const snapMessages = useMemo<ResultMessage[]>(() => {
    if (!snapshot) return []
    return snapshotMessages(snapshot)
  }, [snapshot])

  return (
    <div className="kp-fill" style={{ gap: 12 }}>
      <div className="kp-row">
        <Title3>AI 观测区</Title3>
        <div className="kp-grow" />
        <Button icon={<ArrowClockwiseRegular />} appearance="subtle" onClick={loadSessions}>
          刷新
        </Button>
      </div>

      {Boolean(error) && <ErrorState error={error} />}

      <div className={styles.shell}>
        {/* Sessions */}
        <div className={styles.listWrap}>
          {loading && <LoadingRow />}
          {!loading && sessions.length === 0 && <EmptyState title="无会话" />}
          {sessions.map((s) => {
            const id = sessionId(s)
            return (
              <div
                key={id}
                className={
                  selectedSession === id ? `${styles.listItem} ${styles.listItemActive}` : styles.listItem
                }
                onClick={() => setSelectedSession(id)}
              >
                <Body1 className="kp-cell-mono kp-truncate">{id}</Body1>
                <Body1 className="kp-tertiary" style={{ fontSize: 12 }}>
                  {s.label || '(无标签)'} · {s.op_count} ops
                </Body1>
                <Body1 className="kp-tertiary" style={{ fontSize: 11 }}>
                  {fmtTime(sessionStarted(s))}
                  {sessionEnded(s) ? ` → ${fmtTime(sessionEnded(s))}` : ' (进行中)'}
                </Body1>
              </div>
            )
          })}
        </div>

        {/* Ops timeline + detail */}
        <div className="kp-col" style={{ gap: 12 }}>
          {!selectedSession && <EmptyState title="选择左侧会话查看操作时间线" />}
          {selectedSession && (
            <Card className="kp-card">
              <div className="kp-row" style={{ justifyContent: 'space-between' }}>
                <Subtitle2>
                  <HistoryRegular /> 操作时间线
                </Subtitle2>
                <Body1 className="kp-muted">{ops.length} 个操作</Body1>
              </div>
              <div className={styles.tableWrap} style={{ marginTop: 8 }}>
                <Table size="small">
                  <TableHeader>
                    <TableRow>
                      <TableHeaderCell>Op ID</TableHeaderCell>
                      <TableHeaderCell>Source</TableHeaderCell>
                      <TableHeaderCell>Command</TableHeaderCell>
                      <TableHeaderCell>Status</TableHeaderCell>
                      <TableHeaderCell className="kp-cell-numeric">Duration</TableHeaderCell>
                      <TableHeaderCell>Started</TableHeaderCell>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {opsLoading && (
                      <TableRow>
                        <TableCell colSpan={6}>
                          <LoadingRow />
                        </TableCell>
                      </TableRow>
                    )}
                    {!opsLoading && ops.length === 0 && (
                      <TableRow>
                        <TableCell colSpan={6}>
                          <EmptyState title="无操作记录" />
                        </TableCell>
                      </TableRow>
                    )}
                    {ops.map((op) => {
                      const id = opId(op)
                      const isActive = selectedOp && opId(selectedOp) === id
                      return (
                        <TableRow
                          key={id}
                          style={{ cursor: 'pointer', background: isActive ? tokens.colorBrandBackground2 : undefined }}
                          onClick={() => {
                            setSelectedOp(op)
                            setSnapshot(null)
                          }}
                        >
                          <TableCell className="kp-cell-mono kp-truncate" style={{ maxWidth: 140 }}>
                            {id}
                          </TableCell>
                          <TableCell>{op.source}</TableCell>
                          <TableCell className="kp-cell-mono kp-truncate" style={{ maxWidth: 220 }}>
                            {op.command}
                          </TableCell>
                          <TableCell>
                            <StatusPill status={statusKind(op.status)} label={op.status} />
                          </TableCell>
                          <TableCell className="kp-cell-numeric">
                            {opDuration(op).toLocaleString()} ms
                          </TableCell>
                          <TableCell>{fmtTime(opStarted(op))}</TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            </Card>
          )}

          {selectedOp && (
            <Card className="kp-card">
              <div className="kp-row" style={{ justifyContent: 'space-between' }}>
                <Subtitle2>操作详情</Subtitle2>
                <Button
                  size="small"
                  icon={<PlayCircleRegular />}
                  appearance="subtle"
                  onClick={() => loadSnapshot(selectedOp)}
                  disabled={snapLoading}
                >
                  {snapLoading ? '加载快照…' : '快照重放'}
                </Button>
              </div>
              <Divider style={{ margin: '8px 0' }} />
              <div className={styles.detailSplit}>
                <div className="kp-col">
                  <Body1 className="kp-muted">Op ID</Body1>
                  <Body1 className="kp-cell-mono">{opId(selectedOp)}</Body1>
                  <Body1 className="kp-muted" style={{ marginTop: 8 }}>命令</Body1>
                  <pre className="kp-json" style={{ maxHeight: 120 }}>{selectedOp.command}</pre>
                  {selectedOp.intent && (
                    <>
                      <Body1 className="kp-muted" style={{ marginTop: 8 }}>Intent</Body1>
                      <Body1>{selectedOp.intent}</Body1>
                    </>
                  )}
                  <Body1 className="kp-muted" style={{ marginTop: 8 }}>状态</Body1>
                  <StatusPill status={statusKind(selectedOp.status)} label={selectedOp.status} />
                  {selectedOp.error && (
                    <>
                      <Body1 className="kp-muted" style={{ marginTop: 8 }}>错误</Body1>
                      <Body1 style={{ color: '#c4314b' }}>{selectedOp.error}</Body1>
                    </>
                  )}
                  {selectedOp.warnings && (
                    <>
                      <Body1 className="kp-muted" style={{ marginTop: 8 }}>警告</Body1>
                      <Body1 className="kp-muted">{selectedOp.warnings}</Body1>
                    </>
                  )}
                </div>
                <div className="kp-col">
                  <Body1 className="kp-muted">
                    <EyeRegular /> 结果统计
                  </Body1>
                  <JsonView value={parseSummary(selectedOp.result_summary)} maxHeight={180} />
                  {snapshot && (
                    <>
                      <Body1 className="kp-muted" style={{ marginTop: 8 }}>
                        快照（{snapMessages.length} 条消息{snapshot.truncated ? '，已截断' : ''}）
                      </Body1>
                      <JsonView value={snapMessages} maxHeight={240} />
                    </>
                  )}
                  {!snapshot && !snapLoading && (
                    <Body1 className="kp-tertiary" style={{ marginTop: 8 }}>
                      点击「快照重放」查看扫描命中快照（如存在）。
                    </Body1>
                  )}
                </div>
              </div>
            </Card>
          )}
        </div>
      </div>
    </div>
  )
}
