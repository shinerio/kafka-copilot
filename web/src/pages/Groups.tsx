// Consumer Groups page (FR-02): list groups, select one to inspect lag with
// partition-level detail (committed / hwm / lag).

import {
  Title3,
  Body1,
  Subtitle2,
  Input,
  Field,
  Button,
  Table,
  TableHeader,
  TableHeaderCell,
  TableBody,
  TableRow,
  TableCell,
  Card,
  makeStyles,
  tokens,
} from '@fluentui/react-components'
import {
  ArrowClockwiseRegular,
  PlugConnectedRegular,
  Search24Regular,
} from '@fluentui/react-icons'
import { useCallback, useEffect, useState } from 'react'
import { api } from '../api'
import type { LagInfo } from '../types'
import { EmptyState, ErrorState, LoadingRow, StatusPill, statusKind } from '../components/ui'

const useStyles = makeStyles({
  split: {
    display: 'grid',
    gridTemplateColumns: 'minmax(220px, 1fr) minmax(0, 2fr)',
    gap: '12px',
    alignItems: 'start',
  },
  listWrap: {
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: '8px',
    overflow: 'auto',
    background: tokens.colorNeutralBackground1,
    maxHeight: '70vh',
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
    fontWeight: 600,
  },
  tableWrap: {
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: '8px',
    overflow: 'hidden',
    background: tokens.colorNeutralBackground1,
  },
})

export function GroupsPage({ profile }: { profile: string }) {
  const styles = useStyles()
  const [groups, setGroups] = useState<string[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [lagTopic, setLagTopic] = useState('')
  const [lag, setLag] = useState<LagInfo | null>(null)
  const [loading, setLoading] = useState(false)
  const [lagLoading, setLagLoading] = useState(false)
  const [error, setError] = useState<unknown>(null)
  const [lagError, setLagError] = useState<unknown>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const list = await api.listGroups()
      setGroups(list || [])
    } catch (e) {
      setError(e)
      setGroups([])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const loadLag = useCallback(async () => {
    if (!selected) return
    setLagLoading(true)
    setLagError(null)
    setLag(null)
    try {
      const res = await api.groupLag(selected, lagTopic || undefined)
      setLag(res)
    } catch (e) {
      setLagError(e)
    } finally {
      setLagLoading(false)
    }
  }, [selected, lagTopic])

  useEffect(() => {
    if (selected) void loadLag()
  }, [selected, loadLag])

  const totalLag = lag?.total_lag ?? lag?.totalLag ?? 0
  const highLag = lag?.high_lag ?? lag?.highLag ?? false

  return (
    <div className="kp-fill" style={{ gap: 12 }}>
      <div className="kp-row">
        <Title3>Consumer Groups</Title3>
        {profile && <Body1 className="kp-muted">profile: <b>{profile}</b></Body1>}
        <div className="kp-grow" />
        <Button icon={<ArrowClockwiseRegular />} appearance="subtle" onClick={load}>
          刷新
        </Button>
      </div>

      {Boolean(error) && <ErrorState error={error} />}

      <div className={styles.split}>
        {/* Group list */}
        <div className={styles.listWrap}>
          {loading && <LoadingRow />}
          {!loading && groups.length === 0 && (
            <EmptyState title="无 consumer group" />
          )}
          {groups.map((g) => (
            <div
              key={g}
              className={
                selected === g ? `${styles.listItem} ${styles.listItemActive}` : styles.listItem
              }
              onClick={() => setSelected(g)}
            >
              <div className="kp-row">
                <PlugConnectedRegular />
                <span className="kp-cell-mono kp-truncate">{g}</span>
              </div>
            </div>
          ))}
        </div>

        {/* Lag detail */}
        <Card className="kp-card">
          {!selected && <EmptyState title="选择左侧的 group 查看 lag" />}
          {selected && (
            <>
              <div className="kp-row" style={{ justifyContent: 'space-between' }}>
                <Subtitle2 className="kp-cell-mono">{selected}</Subtitle2>
                <div className="kp-row">
                  <Field>
                    <Input
                      contentBefore={<Search24Regular />}
                      placeholder="topic 过滤（可选）"
                      value={lagTopic}
                      onChange={(_, d) => setLagTopic(d.value)}
                      style={{ width: 200 }}
                    />
                  </Field>
                  <Button size="small" appearance="subtle" onClick={loadLag}>
                    查询
                  </Button>
                </div>
              </div>

              {lagLoading && <LoadingRow />}
              {Boolean(lagError) && <ErrorState error={lagError} />}
              {lag && !lagLoading && (
                <>
                  <div className="kp-row" style={{ marginTop: 8 }}>
                    <Body1 className="kp-muted">Topic: <b>{lag.topic}</b></Body1>
                    <div className="kp-grow" />
                    <Body1 className="kp-muted">Total Lag:</Body1>
                    <StatusPill
                      status={highLag ? statusKind('warning') : totalLag === 0 ? statusKind('ok') : statusKind('muted')}
                      label={totalLag.toLocaleString()}
                    />
                  </div>
                  <div className={styles.tableWrap} style={{ marginTop: 8 }}>
                    <Table size="small">
                      <TableHeader>
                        <TableRow>
                          <TableHeaderCell className="kp-cell-numeric">Partition</TableHeaderCell>
                          <TableHeaderCell className="kp-cell-numeric">Committed</TableHeaderCell>
                          <TableHeaderCell className="kp-cell-numeric">HWM</TableHeaderCell>
                          <TableHeaderCell className="kp-cell-numeric">Lag</TableHeaderCell>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {lag.partitions.map((p) => (
                          <TableRow key={p.partition}>
                            <TableCell className="kp-cell-numeric">{p.partition}</TableCell>
                            <TableCell className="kp-cell-numeric">{p.committed.toLocaleString()}</TableCell>
                            <TableCell className="kp-cell-numeric">{p.hwm.toLocaleString()}</TableCell>
                            <TableCell className="kp-cell-numeric">
                              <span style={{ color: p.lag > 0 ? '#9b5300' : undefined, fontWeight: p.lag > 0 ? 600 : 400 }}>
                                {p.lag.toLocaleString()}
                              </span>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </>
              )}
            </>
          )}
        </Card>
      </div>
    </div>
  )
}
