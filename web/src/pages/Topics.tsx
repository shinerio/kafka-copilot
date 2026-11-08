// Topics page (FR-01): list topics with name, partition count, and each
// partition's latest offset shown as a grouped chip row.

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
} from '@fluentui/react-components'
import {
  Search24Regular,
  ArrowClockwiseRegular,
} from '@fluentui/react-icons'
import { useCallback, useEffect, useState } from 'react'
import { api } from '../api'
import type { TopicInfo } from '../types'
import { EmptyState, ErrorState, LoadingRow } from '../components/ui'

export function TopicsPage({ profile }: { profile: string }) {
  const [topics, setTopics] = useState<TopicInfo[]>([])
  const [filter, setFilter] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<unknown>(null)

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
      <div className="kp-row" style={{ marginBottom: 12 }}>
        <Title3>Topics</Title3>
        <Input
          contentBefore={<Search24Regular />}
          placeholder="按名称过滤…"
          value={filter}
          onChange={(_, d) => setFilter(d.value)}
          style={{ width: 240 }}
        />
        <div className="kp-grow" />
        <Button icon={<ArrowClockwiseRegular />} onClick={load} appearance="subtle">
          刷新
        </Button>
        {profile && <span className="kp-muted">profile: <b>{profile}</b></span>}
      </div>

      <div
        style={{
          border: '1px solid var(--kp-border)',
          borderRadius: 8,
          overflow: 'hidden',
          background: 'var(--kp-bg)',
        }}
      >
        <Table size="small" className="kp-table-fixed">
          <TableHeader>
            <TableRow>
              <TableHeaderCell style={{ width: '26%' }}>Name</TableHeaderCell>
              <TableHeaderCell className="kp-cell-numeric" style={{ width: '12%' }}>
                Partitions
              </TableHeaderCell>
              <TableHeaderCell style={{ width: '62%' }}>各 Partition 最新 Offset</TableHeaderCell>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading && (
              <TableRow>
                <TableCell colSpan={3}>
                  <LoadingRow />
                </TableCell>
              </TableRow>
            )}
            {!loading && Boolean(error) && (
              <TableRow>
                <TableCell colSpan={3}>
                  <ErrorState error={error} />
                </TableCell>
              </TableRow>
            )}
            {!loading && !error && topics.length === 0 && (
              <TableRow>
                <TableCell colSpan={3}>
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
              topics.map((t) => (
                <TableRow key={t.name}>
                  <TableCell className="kp-cell-mono kp-truncate" title={t.name}>
                    {t.name}
                  </TableCell>
                  <TableCell className="kp-cell-numeric">{t.partitions_count}</TableCell>
                  <TableCell>
                    <div className="kp-row" style={{ flexWrap: 'wrap', gap: 6 }}>
                      {(t.partitions && t.partitions.length > 0
                        ? t.partitions
                        : []
                      ).map((p) => (
                        <span key={p.partition} className="kp-chip">
                          P{p.partition}: <b>{p.latest_offset.toLocaleString()}</b>
                        </span>
                      ))}
                      {(!t.partitions || t.partitions.length === 0) && (
                        <span className="kp-muted">—</span>
                      )}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}
