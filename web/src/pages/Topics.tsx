// Topics page (FR-01): list topics with name, partitions_count, latest_offset.
// Clicking a row expands per-partition details.

import {
  Table,
  TableHeader,
  TableHeaderCell,
  TableBody,
  TableRow,
  TableCell,
  Input,
  Button,
  Title3,
  makeStyles,
  tokens,
} from '@fluentui/react-components'
import {
  Search24Regular,
  ArrowClockwiseRegular,
  ChevronRightRegular,
  ChevronDownRegular,
} from '@fluentui/react-icons'
import { useCallback, useEffect, useState, Fragment } from 'react'
import { api } from '../api'
import type { TopicInfo } from '../types'
import { EmptyState, ErrorState, LoadingRow } from '../components/ui'

const useStyles = makeStyles({
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
    marginBottom: '12px',
  },
  tableWrap: {
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: '8px',
    overflow: 'hidden',
    background: tokens.colorNeutralBackground1,
  },
  subRow: {
    background: tokens.colorNeutralBackground2,
  },
})

function latestOffset(t: TopicInfo): number {
  if (!t.partitions || t.partitions.length === 0) return 0
  return t.partitions.reduce((m, p) => (p.latest_offset > m ? p.latest_offset : m), 0)
}

export function TopicsPage({ profile }: { profile: string }) {
  const styles = useStyles()
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [filter, setFilter] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<unknown>(null)
  const [expanded, setExpanded] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const list = await api.listTopics(filter)
      setTopics(list || [])
    } catch (e) {
      setError(e)
      setTopics([])
    } finally {
      setLoading(false)
    }
  }, [filter])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="kp-fill">
      <div className={styles.header}>
        <Title3>Topics</Title3>
        <Input
          contentBefore={<Search24Regular />}
          placeholder="按名称过滤…"
          value={filter}
          onChange={(_, d) => setFilter(d.value)}
          style={{ width: 240 }}
        />
        <div className="kp-grow" />
        <Button
          icon={<ArrowClockwiseRegular />}
          onClick={load}
          appearance="subtle"
        >
          刷新
        </Button>
        {profile && (
          <span className="kp-muted">profile: <b>{profile}</b></span>
        )}
      </div>

      <div className={styles.tableWrap}>
        <Table size="small">
          <TableHeader>
            <TableRow>
              <TableHeaderCell style={{ width: 32 }} />
              <TableHeaderCell>Name</TableHeaderCell>
              <TableHeaderCell className="kp-cell-numeric">Partitions</TableHeaderCell>
              <TableHeaderCell className="kp-cell-numeric">Latest Offset</TableHeaderCell>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading && (
              <TableRow>
                <TableCell colSpan={4}>
                  <LoadingRow />
                </TableCell>
              </TableRow>
            )}
            {!loading && Boolean(error) && (
              <TableRow>
                <TableCell colSpan={4}>
                  <ErrorState error={error} />
                </TableCell>
              </TableRow>
            )}
            {!loading && !error && topics.length === 0 && (
              <TableRow>
                <TableCell colSpan={4}>
                  <EmptyState
                    title="未找到 topic"
                    hint={
                      profile
                        ? '确认 Kafka 集群可达并已选择正确的 profile。'
                        : '请先在「设置」中添加并选择一个 profile。'
                    }
                  />
                </TableCell>
              </TableRow>
            )}
            {!loading &&
              !error &&
              topics.map((t) => {
                const isOpen = expanded === t.name
                return (
                  <Fragment key={t.name}>
                    <TableRow
                      style={{ cursor: 'pointer' }}
                      onClick={() => setExpanded(isOpen ? null : t.name)}
                    >
                      <TableCell>
                        {isOpen ? (
                          <ChevronDownRegular />
                        ) : (
                          <ChevronRightRegular />
                        )}
                      </TableCell>
                      <TableCell className="kp-cell-mono">{t.name}</TableCell>
                      <TableCell className="kp-cell-numeric">{t.partitions_count}</TableCell>
                      <TableCell className="kp-cell-numeric">
                        {latestOffset(t).toLocaleString()}
                      </TableCell>
                    </TableRow>
                    {isOpen && (
                      <TableRow className={styles.subRow}>
                        <TableCell />
                        <TableCell colSpan={3}>
                          <div className="kp-row" style={{ flexWrap: 'wrap' }}>
                            {t.partitions?.map((p) => (
                              <div
                                key={p.partition}
                                className="kp-card"
                                style={{
                                  padding: '8px 12px',
                                  minWidth: 130,
                                  display: 'flex',
                                  flexDirection: 'column',
                                  gap: 2,
                                }}
                              >
                                <span className="kp-tertiary" style={{ fontSize: 12 }}>
                                  Partition {p.partition}
                                </span>
                                <span className="kp-cell-numeric" style={{ fontWeight: 600 }}>
                                  {p.latest_offset.toLocaleString()}
                                </span>
                              </div>
                            ))}
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
    </div>
  )
}
