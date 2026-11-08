// Message scan page (FR-04/05): build a scan query, show total/matched/elapsed
// stats, then render matched messages in a paginated table with row expansion
// to a JSON view of key/value/headers.
//
// Layout note (#3): the query form + stats stay fixed (flex-shrink:0) while the
// results table scrolls inside its own bounded container, so the scan button is
// never pushed off-screen when there are many results.

import {
  Title3,
  Body1,
  Subtitle2,
  Field,
  Button,
  Table,
  TableHeader,
  TableHeaderCell,
  TableBody,
  TableRow,
  TableCell,
  Card,
  tokens,
  makeStyles,
  MessageBar,
  MessageBarBody,
  Switch,
} from '@fluentui/react-components'
import { ChevronRightRegular, ChevronDownRegular } from '@fluentui/react-icons'
import { Fragment, useCallback, useEffect, useMemo, useState } from 'react'
import { api } from '../api'
import type { KeyPartitionInfo, ResultMessage, ScanResult } from '../types'
import { EmptyState, ErrorState, JsonView, Pagination, StatStrip, statusKind } from '../components/ui'
import { TopicSelect, PartitionMultiSelect, DateTimeField } from '../components/controls'

const PAGE_SIZE = 50

// Backscan amount is a fixed set of radio options (FR-05). MaxLast on the
// backend is 10000, so 10000 is the largest selectable option.
const LAST_OPTIONS = [100, 500, 1000, 5000, 10000] as const

const useStyles = makeStyles({
  formGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
    gap: '12px',
    alignItems: 'end',
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
    overflow: 'auto',
    background: tokens.colorNeutralBackground1,
    flex: '1 1 auto',
    minHeight: 0,
  },
  jsonCell: {
    background: tokens.colorNeutralBackground2,
  },
  resultsArea: {
    display: 'flex',
    flexDirection: 'column',
    flex: '1 1 auto',
    minHeight: 0,
    gap: '12px',
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

type HeaderMap = Record<string, string>

function headerEntries(h?: HeaderMap): [string, string][] {
  return Object.entries(h || {})
}

// Empty values render as "" so "k=" is never mistaken for a missing value.
// The column preview clips long values; the tooltip keeps them intact.
function headerValue(v: string, clip = 0): string {
  if (v === '') return '""'
  if (clip > 0 && v.length > clip) return `${v.slice(0, clip)}…`
  return v
}

// Compact preview for the Headers column: show up to two pairs, then a count.
function headersPreview(h?: HeaderMap): string {
  const entries = headerEntries(h)
  if (entries.length === 0) return '—'
  const head = entries
    .slice(0, 2)
    .map(([k, v]) => `${k}=${headerValue(v, 28)}`)
    .join(', ')
  return entries.length > 2 ? `${head} +${entries.length - 2}` : head
}

// Full content for the native tooltip, including binary values shown as hex.
function headersTitle(h?: HeaderMap): string {
  const entries = headerEntries(h)
  return entries.length === 0
    ? '（无 headers）'
    : entries.map(([k, v]) => `${k}=${headerValue(v)}`).join('\n')
}

// Warning codes are kept machine-readable in the API / CLI / audit log, but
// must be spelled out in the UI: a bare "last-truncated" tells users nothing
// about why they see 100 rows after asking for 100.
function explainWarnings(result: ScanResult): string[] {
  const raw = result.matched_raw
  return (result.warnings || []).map((code) => {
    switch (code) {
      // Legacy warning from versions that globally truncated hits to --last.
      // Kept so results from an older service / audit snapshot still read well.
      case 'last-truncated':
        return raw && raw > result.matched
          ? `结果已被旧版全局截断：本次共命中 ${raw.toLocaleString()} 条，只返回时间最新的 ${result.matched.toLocaleString()} 条。当前版本会返回各分区全部命中结果。`
          : `结果已被旧版全局截断，只返回时间最新的 ${result.matched.toLocaleString()} 条。`
      case 'result-cap':
        return `命中数 ${raw?.toLocaleString() ?? ''} 超过单次返回安全上限 50,000 条，已按分区均摊保留（每个分区各保留最新的若干条，所有分区仍有结果），共返回 ${result.matched.toLocaleString()} 条。想看全量请缩小回扫量、加过滤条件，或只选部分分区。`
      case 'key-partition':
        return result.key_partition === undefined
          ? '本次按 key 哈希定位只扫描了单个分区，其它分区未检查。'
          : `本次按 key 哈希定位仅扫描 P${result.key_partition}（共 ${result.key_partition_count ?? '?'} 个分区，其余 ${Math.max(0, (result.key_partition_count ?? 1) - 1)} 个分区未检查）。若生产者用了自定义分区器 / 显式分区，或 topic 曾扩过分区，结果可能漏数据；不确定请关闭「按 key 定位分区」重扫全部分区。`
      case 'scan>1000':
        return '回扫量较大（每分区档位 >1,000），扫描可能耗时较长。'
      case 'timeout':
        return '扫描超时，返回的是已扫描到的部分结果，可能不完整。'
      default:
        return code
    }
  })
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
  const [partitions, setPartitions] = useState<number[]>([])
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<ScanResult | null>(null)
  const [error, setError] = useState<unknown>(null)
  const [page, setPage] = useState(1)
  const [expandedRow, setExpandedRow] = useState<number | null>(null)
  // Result-set partition filter: 'all' by default so every partition's hits
  // stay visible; clicking a chip narrows the table to that partition.
  const [resultPartition, setResultPartition] = useState<number | 'all'>('all')

  // Explicit key-location switch (FR-05). Off by default: a wrong partition
  // hint silently drops whole partitions, which is worse than a slower full
  // scan. The server is authoritative -- this state is only for display.
  const [keyLocate, setKeyLocate] = useState(false)
  const [keyLocInfo, setKeyLocInfo] = useState<KeyPartitionInfo | null>(null)
  const [keyLocError, setKeyLocError] = useState('')
  const [keyLocLoading, setKeyLocLoading] = useState(false)

  const keyLocEligible = key.length > 0 && topic.length > 0
  const keyLocActive = keyLocate && keyLocEligible && keyLocInfo !== null

  useEffect(() => {
    if (!keyLocate || !keyLocEligible) {
      setKeyLocInfo(null)
      setKeyLocError('')
      setKeyLocLoading(false)
      return
    }
    let cancelled = false
    setKeyLocLoading(true)
    // Debounce so typing a key does not fire one metadata request per char.
    const timer = window.setTimeout(() => {
      api
        .keyPartition(topic, key)
        .then((info) => {
          if (cancelled) return
          setKeyLocInfo(info)
          setKeyLocError('')
          setPartitions([info.partition])
        })
        .catch((e) => {
          if (cancelled) return
          setKeyLocInfo(null)
          setKeyLocError(e instanceof Error ? e.message : String(e))
        })
        .finally(() => {
          if (!cancelled) setKeyLocLoading(false)
        })
    }, 250)
    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [keyLocate, keyLocEligible, topic, key])

  const toggleKeyLocate = useCallback((on: boolean) => {
    setKeyLocate(on)
    if (!on) {
      setKeyLocInfo(null)
      setKeyLocError('')
      setKeyLocLoading(false)
      setPartitions([]) // restore "all partitions"
    }
  }, [])

  const messages = useMemo<ResultMessage[]>(() => result?.messages || [], [result])
  const warningTexts = useMemo(() => (result ? explainWarnings(result) : []), [result])
  const partitionCount = result?.partition_ranges?.length ?? 0

  const partitionOptions = useMemo(() => {
    const counts = new Map<number, number>()
    for (const m of messages) counts.set(m.partition, (counts.get(m.partition) || 0) + 1)
    return [...counts.entries()].sort((a, b) => a[0] - b[0])
  }, [messages])

  const visibleMessages = useMemo(
    () => (resultPartition === 'all' ? messages : messages.filter((m) => m.partition === resultPartition)),
    [messages, resultPartition],
  )

  const pickResultPartition = useCallback((p: number | 'all') => {
    setResultPartition(p)
    setPage(1)
    setExpandedRow(null)
  }, [])

  const onTopicChange = useCallback((v: string) => {
    setTopic(v)
    setPartitions([]) // reset partition selection when topic changes
  }, [])

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
    setResultPartition('all')
    try {
      const res = await api.scan({
        topic,
        last,
        since: since || undefined,
        until: until || undefined,
        key: key || undefined,
        keyword: keyword || undefined,
        expr: expr || undefined,
        // When key location is on, the server computes the partition itself;
        // send no explicit partition list so the backend stays authoritative.
        partitions:
          keyLocate && keyLocEligible && keyLocInfo
            ? undefined
            : partitions.length
              ? partitions
              : undefined,
        key_partition: keyLocate && keyLocEligible && keyLocInfo ? true : undefined,
      })
      setResult(res)
    } catch (e) {
      setError(e)
    } finally {
      setRunning(false)
    }
  }, [topic, last, since, until, key, keyword, expr, partitions, keyLocate, keyLocEligible, keyLocInfo])

  const pageRows = useMemo(
    () => visibleMessages.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE),
    [visibleMessages, page],
  )

  return (
    <div className="kp-fill kp-fill--scan">
      <div className="kp-row">
        <Title3>消息查询</Title3>
        {profile && <Body1 className="kp-muted">profile: <b>{profile}</b></Body1>}
        <div className="kp-grow" />
      </div>

      {/* Query form — fixed, never scrolls away */}
      <Card className="kp-card" style={{ flexShrink: 0 }}>
        <Subtitle2>查询条件</Subtitle2>
        <div className={styles.formGrid} style={{ marginTop: 8 }}>
          <Field label="Topic *">
            <TopicSelect value={topic} onChange={onTopicChange} />
          </Field>
          <Field
            label={
              keyLocActive && keyLocInfo
                ? `分区（已按 key 定位为 P${keyLocInfo.partition}）`
                : '分区 (多选, 默认全部分区)'
            }
          >
            <div className={keyLocActive ? 'kp-keyloc__locked' : undefined}>
              <PartitionMultiSelect
                topic={topic}
                value={keyLocActive && keyLocInfo ? [keyLocInfo.partition] : partitions}
                onChange={setPartitions}
              />
            </div>
          </Field>
          <Field label="--key (精确)">
            <input
              className="kp-datetime"
              style={{ height: 32, fontFamily: 'var(--kp-font-mono)' }}
              value={key}
              placeholder="精确 key"
              onChange={(e) => setKey(e.target.value)}
            />
          </Field>
          <Field label="--keyword (支持 AND/OR/NOT 与括号)">
            <input
              className="kp-datetime"
              style={{ height: 32, fontFamily: 'var(--kp-font-mono)' }}
              value={keyword}
              placeholder='如: FAILED AND (amount>100 OR retry)'
              onChange={(e) => setKeyword(e.target.value)}
            />
          </Field>
          <Field label="--expr (jq, 作用于消息体)">
            <input
              className="kp-datetime"
              style={{ height: 32, fontFamily: 'var(--kp-font-mono)' }}
              value={expr}
              placeholder=".amount == 352"
              onChange={(e) => setExpr(e.target.value)}
            />
          </Field>
          <DateTimeField label="Since" value={since} onChange={setSince} />
          <DateTimeField label="Until" value={until} onChange={setUntil} />
        </div>
        {/* Explicit, visible key-location switch. Off by default; the risk
            list spells out why "no partition expansion" is necessary but NOT
            sufficient for a complete result. */}
        <div className="kp-keyloc">
          <div className="kp-keyloc__row">
            <Switch
              checked={keyLocate}
              onChange={(_, d) => toggleKeyLocate(d.checked)}
              disabled={!keyLocEligible}
              label="按 key 定位分区（实验性）"
            />
            <span className="kp-keyloc__hint">
              {keyLocEligible ? `精确 key：${key}` : '需先填写 Topic 与精确 --key 才能启用'}
              {' · 默认关闭，扫描全部分区'}
            </span>
          </div>
          {keyLocate && keyLocEligible && (
            <>
              {keyLocLoading && (
                <Body1 className="kp-muted kp-keyloc__status">正在计算分区…</Body1>
              )}
              {keyLocError && (
                <Body1 className="kp-keyloc__error">分区计算失败：{keyLocError}</Body1>
              )}
              {keyLocInfo && (
                <>
                  <div className="kp-keyloc__result">
                    <span className="kp-keyloc__badge">key → P{keyLocInfo.partition}</span>
                    <span>
                      共 {keyLocInfo.partition_count} 个分区，本次只扫描 P{keyLocInfo.partition}
                      ，跳过其余 {Math.max(0, keyLocInfo.partition_count - 1)} 个
                    </span>
                    <code className="kp-keyloc__algo">{keyLocInfo.algorithm}</code>
                  </div>
                  <div className="kp-keyloc__risk">
                    <div className="kp-keyloc__risk-title">
                      风险提示：分区数没变，也不代表一定不漏数据
                    </div>
                    只有以下条件<b>全部</b>满足时才不会漏：
                    <ol>
                      <li>topic 分区数从未变化（未扩分区、未删除重建）；</li>
                      <li>所有生产者都用标准 murmur2 分区器（Java / franz-go 默认），没有自定义分区器、没有显式指定分区；</li>
                      <li>写入与查询的 key 字节完全一致（编码、大小写、空格、二进制内容）。</li>
                    </ol>
                    任一条不确定，请关闭本开关重扫全部分区。key 正则 / keyword / jq 查询无法用此方式定位；本功能只对精确 --key 生效。
                  </div>
                </>
              )}
            </>
          )}
        </div>
        {/* Kept outside the auto-fit grid: a 2-column span on a Fluent Field
            mis-places it and overlaps the Topic combo, hiding the 100 option. */}
        <div className="kp-last-row">
          <span className="kp-last-row__label">回扫量 (--last)</span>
          <div className="kp-last-radios" role="radiogroup" aria-label="回扫量">
            {LAST_OPTIONS.map((n) => (
              <label
                key={n}
                className={`kp-last-radio${last === n ? ' is-checked' : ''}`}
                title={`回扫最近 ${n.toLocaleString()} 条`}
              >
                <input
                  type="radio"
                  name="kp-last"
                  value={n}
                  checked={last === n}
                  onChange={() => setLast(n)}
                />
                <span>{n.toLocaleString()}</span>
              </label>
            ))}
          </div>
        </div>
        <Body1 className="kp-muted" style={{ fontSize: 12, marginTop: 4 }}>
          每个分区各回扫所选条数（例：12 个分区选 100 → 实际扫描约 1,200 条）；各分区的命中结果全部保留、不再全局截断，可在下方结果区按分区筛选查看。
        </Body1>
        <div className={styles.toolbar}>
          <Button appearance="primary" onClick={run} disabled={running || !topic}>
            {running ? '扫描中…' : '扫描'}
          </Button>
          {running && <span className="kp-spinner" />}
          {last > 1000 && (
            <Body1 className="kp-muted kp-last-warning">
              回扫 {last.toLocaleString()} 条，可能耗时较长
            </Body1>
          )}
          {(since || until) && (
            <Body1 className="kp-muted">时间窗口已启用</Body1>
          )}
        </div>
      </Card>

      {Boolean(error) && <ErrorState error={error} />}

      {result && (
        <div className={styles.resultsArea}>
          {/* Stats — fixed */}
          <Card className="kp-card" style={{ flexShrink: 0 }}>
            <div className="kp-row" style={{ justifyContent: 'space-between' }}>
              <StatStrip
                stats={[
                  { label: '已扫描', value: result.total.toLocaleString() },
                  {
                    label: result.matched_raw && result.matched_raw > result.matched
                      ? `命中（共 ${result.matched_raw.toLocaleString()} / 返回 ${result.matched.toLocaleString()}）`
                      : '命中（已返回）',
                    value: result.matched.toLocaleString(),
                    tone: statusKind(result.truncated ? 'truncated' : 'ok'),
                  },
                  { label: '耗时 (ms)', value: result.elapsedMs.toLocaleString() },
                  { label: 'Op ID', value: <span className="kp-mono">{result.opId}</span> },
                ]}
              />
              {result.truncated && <Body1 className="kp-muted">结果已截断</Body1>}
            </div>
            <Body1 className="kp-muted" style={{ fontSize: 12, marginTop: 6 }}>
              已扫描 = 实际读取并检查的原始消息数（不是 topic 总消息数）。
              {partitionCount > 0
                ? ` 本次覆盖 ${partitionCount} 个分区；回扫量是「每个分区各回扫 N 条」，所以总量约为 分区数 × N。`
                : ' 回扫量是「每个分区各回扫 N 条」，总量约为 分区数 × N。'}
              {' '}命中 = 通过过滤后的全部消息，按时间倒序跨分区归并；每个分区的结果都保留，不再全局截断，可在结果区按分区筛选。
            </Body1>
            {result.key_partition !== undefined && (
              <div className="kp-keyloc__scanned">
                本次实际扫描：<b>P{result.key_partition}</b>（共 {result.key_partition_count ?? '?'} 个分区，其余 {Math.max(0, (result.key_partition_count ?? 1) - 1)} 个未检查）。命中数只代表该分区，其它分区可能仍有相同 key 的消息。
              </div>
            )}
            {warningTexts.length > 0 && (
              <MessageBar intent="warning" style={{ marginTop: 8 }}>
                <MessageBarBody>{warningTexts.join('；')}</MessageBarBody>
              </MessageBar>
            )}
          </Card>

          {/* Result-set partition filter: all partitions are returned; the user
              can narrow the table without re-running the scan. */}
          {partitionOptions.length > 0 && (
            <div className="kp-result-pfilter">
              <span className="kp-result-pfilter__label">结果分区</span>
              <button
                type="button"
                className={`kp-result-pfilter__chip${resultPartition === 'all' ? ' is-active' : ''}`}
                onClick={() => pickResultPartition('all')}
                title="显示全部已扫描分区的命中结果"
              >
                全部 ({messages.length.toLocaleString()})
              </button>
              {partitionOptions.map(([p, n]) => (
                <button
                  key={p}
                  type="button"
                  className={`kp-result-pfilter__chip${resultPartition === p ? ' is-active' : ''}`}
                  onClick={() => pickResultPartition(p)}
                  title={`只看分区 ${p} 的 ${n.toLocaleString()} 条命中`}
                >
                  P{p} ({n.toLocaleString()})
                </button>
              ))}
              {partitionOptions.length > 1 && (
                <span className="kp-muted kp-result-pfilter__hint">
                  默认展示全部分区，点分区号只看该分区（无需重新扫描）
                </span>
              )}
            </div>
          )}

          {/* Results — scrolls inside its own container */}
          <div className={styles.tableWrap}>
            <Table size="small">
              <TableHeader>
                <TableRow>
                  <TableHeaderCell style={{ width: 32 }} />
                  <TableHeaderCell>Key</TableHeaderCell>
                  <TableHeaderCell>Value</TableHeaderCell>
                  <TableHeaderCell>Headers</TableHeaderCell>
                  <TableHeaderCell className="kp-cell-numeric">Partition</TableHeaderCell>
                  <TableHeaderCell className="kp-cell-numeric">Offset</TableHeaderCell>
                  <TableHeaderCell>Timestamp</TableHeaderCell>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pageRows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={7}>
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
                        <TableCell
                          className={`kp-cell-mono kp-truncate${
                            headerEntries(m.headers).length === 0 ? ' kp-muted' : ''
                          }`}
                          style={{ maxWidth: 220 }}
                          title={headersTitle(m.headers)}
                        >
                          {headersPreview(m.headers)}
                        </TableCell>
                        <TableCell className="kp-cell-numeric">{m.partition}</TableCell>
                        <TableCell className="kp-cell-numeric">{m.offset.toLocaleString()}</TableCell>
                        <TableCell>{fmtTs(m.timestamp)}</TableCell>
                      </TableRow>
                      {isOpen && (
                        <TableRow className={styles.jsonCell}>
                          <TableCell />
                          <TableCell colSpan={6}>
                            <Subtitle2>消息内容</Subtitle2>
                            <JsonView
                              value={{
                                key: m.key,
                                value: m.value,
                                partition: m.partition,
                                offset: m.offset,
                                timestamp: m.timestamp,
                                size: m.size,
                                truncated: m.truncated,
                              }}
                            />
                            <div style={{ marginTop: 10 }}>
                              <Subtitle2>
                                Headers（{headerEntries(m.headers).length} 项）
                              </Subtitle2>
                              {headerEntries(m.headers).length > 0 ? (
                                <JsonView value={m.headers} maxHeight={220} />
                              ) : (
                                <Body1 className="kp-muted" style={{ fontSize: 12 }}>
                                  该消息无 headers
                                </Body1>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  )
                })}
              </TableBody>
            </Table>
          </div>

          <div style={{ flexShrink: 0 }}>
            <Pagination page={page} pageSize={PAGE_SIZE} total={visibleMessages.length} onPage={setPage} />
            <Body1 className="kp-muted" style={{ fontSize: 12, marginTop: 4 }}>
              {resultPartition === 'all'
                ? `全部分区共 ${visibleMessages.length.toLocaleString()} 条命中，每页 ${PAGE_SIZE} 条`
                : `分区 P${resultPartition} 共 ${visibleMessages.length.toLocaleString()} 条命中（全部分区共 ${messages.length.toLocaleString()} 条），每页 ${PAGE_SIZE} 条`}
            </Body1>
          </div>
        </div>
      )}
    </div>
  )
}
