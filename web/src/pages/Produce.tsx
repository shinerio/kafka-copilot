// Data injection page (FR-06/07): single-message produce + script batch
// produce with dry-run preview and execution progress. While a batch runs we
// also listen to the audit SSE stream for op.progress events to surface
// produced/failed counts live.

import {
  Title3,
  Body1,
  Subtitle2,
  Input,
  Field,
  Textarea,
  Button,
  SpinButton,
  Tab,
  TabList,
  Card,
  makeStyles,
  tokens,
  MessageBar,
  MessageBarBody,
  MessageBarTitle,
} from '@fluentui/react-components'
import {
  RocketRegular,
  PlayRegular,
  EyeRegular,
  AttachRegular,
} from '@fluentui/react-icons'
import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '../api'
import type { BatchResult } from '../types'
import { EmptyState, ErrorState, JsonView, ProgressBar } from '../components/ui'

const useStyles = makeStyles({
  formGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
    gap: '12px',
  },
  toolbar: {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    marginTop: '12px',
  },
  fileLabel: {
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
    padding: '6px 12px',
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: '4px',
    cursor: 'pointer',
    background: tokens.colorNeutralBackground1,
    '&:hover': { background: tokens.colorNeutralBackground1Hover },
  },
})

interface HeaderRow {
  k: string
  v: string
}

export function ProducePage({ profile }: { profile: string }) {
  const styles = useStyles()
  const [tab, setTab] = useState<'single' | 'batch'>('single')

  // --- single ---
  const [sTopic, setSTopic] = useState('')
  const [sKey, setSKey] = useState('')
  const [sValue, setSValue] = useState('')
  const [sPartition, setSPartition] = useState(-1)
  const [sHeaders, setSHeaders] = useState<HeaderRow[]>([{ k: '', v: '' }])
  const [sResult, setSResult] = useState<unknown>(null)
  const [sError, setSError] = useState<unknown>(null)
  const [sRunning, setSRunning] = useState(false)

  // --- batch ---
  const [bTopic, setBTopic] = useState('')
  const [bScriptName, setBScriptName] = useState('')
  const [bScript, setBScript] = useState('')
  const [bCount, setBCount] = useState(100)
  const [bConcurrency, setBConcurrency] = useState(8)
  const [bRate, setBRate] = useState(0)
  const [bResult, setBResult] = useState<BatchResult | null>(null)
  const [bError, setBError] = useState<unknown>(null)
  const [bRunning, setBRunning] = useState(false)
  const [bProgress, setBProgress] = useState<{ produced: number; failed: number } | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  // SSE subscription during batch run.
  useEffect(() => {
    if (!bRunning) return
    const es = api.auditStream({
      onEvent: (ev) => {
        try {
          const data = JSON.parse(ev.data)
          if (typeof data.produced === 'number' || typeof data.failed === 'number') {
            setBProgress({ produced: data.produced || 0, failed: data.failed || 0 })
          }
        } catch {
          /* ignore non-JSON heartbeat */
        }
      },
    })
    return () => es.close()
  }, [bRunning])

  const onFile = useCallback((file?: File) => {
    if (!file) return
    setBScriptName(file.name)
    const reader = new FileReader()
    reader.onload = () => setBScript(String(reader.result || ''))
    reader.readAsText(file)
  }, [])

  const produceSingle = useCallback(async () => {
    if (!sTopic || !sValue) {
      setSError(new Error('topic 和 value 必填'))
      return
    }
    setSRunning(true)
    setSError(null)
    setSResult(null)
    try {
      const headers: Record<string, string> = {}
      for (const h of sHeaders) {
        if (h.k.trim()) headers[h.k.trim()] = h.v
      }
      const res = await api.produceSingle({
        topic: sTopic,
        key: sKey || undefined,
        value: sValue,
        headers: Object.keys(headers).length ? headers : undefined,
        partition: sPartition >= 0 ? sPartition : undefined,
      })
      setSResult(res)
    } catch (e) {
      setSError(e)
    } finally {
      setSRunning(false)
    }
  }, [sTopic, sKey, sValue, sHeaders, sPartition])

  const runBatch = useCallback(
    async (dryRun: boolean) => {
      if (!bTopic || !bScript) {
        setBError(new Error('topic 和脚本必填'))
        return
      }
      setBRunning(true)
      setBError(null)
      setBResult(null)
      setBProgress(null)
      try {
        const res = await api.produceBatch({
          topic: bTopic,
          script: bScript,
          count: bCount,
          concurrency: bConcurrency,
          rate: bRate,
          dry_run: dryRun,
        })
        setBResult(res)
      } catch (e) {
        setBError(e)
      } finally {
        setBRunning(false)
      }
    },
    [bTopic, bScript, bCount, bConcurrency, bRate],
  )

  const pct =
    bProgress && bCount > 0
      ? Math.min(1, (bProgress.produced + bProgress.failed) / bCount)
      : undefined

  return (
    <div className="kp-fill" style={{ gap: 12 }}>
      <div className="kp-row">
        <Title3>数据注入</Title3>
        {profile && <Body1 className="kp-muted">profile: <b>{profile}</b></Body1>}
        <div className="kp-grow" />
      </div>

      <TabList selectedValue={tab} onTabSelect={(_, d) => setTab(d.value as 'single' | 'batch')}>
        <Tab value="single">单条注入</Tab>
        <Tab value="batch">脚本批量注入</Tab>
      </TabList>

      {tab === 'single' && (
        <Card className="kp-card">
          <Subtitle2>
            <RocketRegular /> 单条消息
          </Subtitle2>
          <div className={styles.formGrid} style={{ marginTop: 8 }}>
            <Field label="Topic *">
              <Input value={sTopic} onChange={(_, d) => setSTopic(d.value)} />
            </Field>
            <Field label="Key">
              <Input value={sKey} onChange={(_, d) => setSKey(d.value)} />
            </Field>
            <Field label="Partition (-1 = 自动)">
              <SpinButton
                value={sPartition}
                min={-1}
                onChange={(_, d) => setSPartition(Number(d.value) || -1)}
              />
            </Field>
          </div>
          <Field label="Value *" style={{ marginTop: 12 }}>
            <Textarea
              value={sValue}
              onChange={(_, d) => setSValue(d.value)}
              textarea={{ style: { minHeight: 100, fontFamily: 'var(--kp-font-mono)' } }}
              placeholder="消息内容（文本或 JSON）"
            />
          </Field>
          <div style={{ marginTop: 12 }}>
            <Body1 className="kp-muted">Headers</Body1>
            {sHeaders.map((h, i) => (
              <div key={i} className="kp-row" style={{ marginTop: 4 }}>
                <Input
                  placeholder="header 名"
                  value={h.k}
                  onChange={(_, d) =>
                    setSHeaders((prev) =>
                      prev.map((x, j) => (j === i ? { ...x, k: d.value } : x)),
                    )
                  }
                  style={{ width: 180 }}
                />
                <Input
                  placeholder="header 值"
                  value={h.v}
                  onChange={(_, d) =>
                    setSHeaders((prev) =>
                      prev.map((x, j) => (j === i ? { ...x, v: d.value } : x)),
                    )
                  }
                  style={{ flex: 1 }}
                />
                <Button
                  size="small"
                  appearance="subtle"
                  onClick={() => setSHeaders((prev) => prev.filter((_, j) => j !== i))}
                >
                  删除
                </Button>
              </div>
            ))}
            <Button
              size="small"
              appearance="subtle"
              onClick={() => setSHeaders((prev) => [...prev, { k: '', v: '' }])}
              style={{ marginTop: 4 }}
            >
              + 添加 header
            </Button>
          </div>
          <div className={styles.toolbar}>
            <Button
              appearance="primary"
              icon={<PlayRegular />}
              onClick={produceSingle}
              disabled={sRunning || !sTopic || !sValue}
            >
              {sRunning ? '发送中…' : '发送'}
            </Button>
            {sRunning && <span className="kp-spinner" />}
          </div>

          {Boolean(sError) && <ErrorState error={sError} />}
          {sResult && (
            <div style={{ marginTop: 12 }}>
              <MessageBar intent="success">
                <MessageBarBody>
                  <MessageBarTitle>已发送</MessageBarTitle>
                  已写入 partition {String((sResult as { partition: number }).partition)}，
                  offset {(sResult as { offset: number }).offset.toLocaleString()}
                </MessageBarBody>
              </MessageBar>
              <JsonView value={sResult} />
            </div>
          )}
        </Card>
      )}

      {tab === 'batch' && (
        <Card className="kp-card">
          <Subtitle2>
            <RocketRegular /> 脚本批量注入
          </Subtitle2>
          <div className={styles.formGrid} style={{ marginTop: 8 }}>
            <Field label="Topic *">
              <Input value={bTopic} onChange={(_, d) => setBTopic(d.value)} />
            </Field>
            <Field label="Count">
              <SpinButton value={bCount} min={1} onChange={(_, d) => setBCount(Number(d.value) || 1)} />
            </Field>
            <Field label="Concurrency">
              <SpinButton
                value={bConcurrency}
                min={1}
                max={32}
                onChange={(_, d) => setBConcurrency(Number(d.value) || 1)}
              />
            </Field>
            <Field label="Rate (msgs/s, 0=不限)">
              <SpinButton value={bRate} min={0} onChange={(_, d) => setBRate(Number(d.value) || 0)} />
            </Field>
          </div>
          <div className="kp-row" style={{ marginTop: 12, alignItems: 'flex-start' }}>
            <label className={styles.fileLabel}>
              <AttachRegular />
              {bScriptName || '选择脚本文件…'}
              <input
                ref={fileRef}
                type="file"
                accept=".js,.mjs,.txt"
                style={{ display: 'none' }}
                onChange={(e) => onFile(e.target.files?.[0] || undefined)}
              />
            </label>
            {bScriptName && <Body1 className="kp-muted">{bScript.length.toLocaleString()} 字节</Body1>}
          </div>
          {bScript && (
            <pre className="kp-json" style={{ marginTop: 8, maxHeight: 160 }}>
              {bScript.slice(0, 4000)}
              {bScript.length > 4000 ? '\n…（已截断预览）' : ''}
            </pre>
          )}
          <div className={styles.toolbar}>
            <Button
              icon={<EyeRegular />}
              onClick={() => runBatch(true)}
              disabled={bRunning || !bTopic || !bScript}
            >
              Dry-run 预览
            </Button>
            <Button
              appearance="primary"
              icon={<PlayRegular />}
              onClick={() => runBatch(false)}
              disabled={bRunning || !bTopic || !bScript}
            >
              {bRunning ? '执行中…' : '执行'}
            </Button>
            {bRunning && <span className="kp-spinner" />}
          </div>

          {bRunning && (
            <div style={{ marginTop: 12 }}>
              <ProgressBar
                indeterminate={pct == null}
                value={pct}
                label={
                  bProgress
                    ? `已产生 ${bProgress.produced.toLocaleString()} / 失败 ${bProgress.failed.toLocaleString()}`
                    : '执行中…'
                }
              />
            </div>
          )}

          {Boolean(bError) && <ErrorState error={bError} />}
          {bResult && (
            <div style={{ marginTop: 12 }}>
              {bResult.dryRun ? (
                <MessageBar intent="info">
                  <MessageBarBody>
                    <MessageBarTitle>Dry-run 预览</MessageBarTitle>
                    生成 {bResult.samples?.length || 0} 条样本（未实际发送）
                  </MessageBarBody>
                </MessageBar>
              ) : (
                <MessageBar intent={bResult.failed > 0 ? 'warning' : 'success'}>
                  <MessageBarBody>
                    <MessageBarTitle>完成</MessageBarTitle>
                    成功 {bResult.produced.toLocaleString()}，失败 {bResult.failed.toLocaleString()}，
                    TPS {bResult.tps.toFixed(1)}，耗时 {bResult.elapsedMs} ms
                  </MessageBarBody>
                </MessageBar>
              )}
              <JsonView value={bResult} />
            </div>
          )}
          {!bResult && !bRunning && !bError && (
            <EmptyState title="尚未执行" hint="填写参数后点击 Dry-run 预览或执行。" />
          )}
        </Card>
      )}
    </div>
  )
}
