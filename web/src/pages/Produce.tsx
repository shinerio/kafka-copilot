// Data injection page (FR-06/07): single-message produce + script batch
// produce with dry-run preview and execution progress.
// The message key is mandatory for single produce: the target partition is
// derived from the key by franz-go, users never pick a partition manually.

import {
  Title3,
  Body1,
  Subtitle2,
  Input,
  Field,
  Textarea,
  Button,
  Tab,
  TabList,
  Card,
  makeStyles,
  MessageBar,
  MessageBarBody,
  MessageBarTitle,
} from '@fluentui/react-components'
import {
  RocketRegular,
  PlayRegular,
  EyeRegular,
} from '@fluentui/react-icons'
import { useCallback, useEffect, useState } from 'react'
import { api } from '../api'
import type { BatchResult } from '../types'
import { EmptyState, ErrorState, JsonView, ProgressBar } from '../components/ui'
import { TopicSelect } from '../components/controls'

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
})

// Runnable goja examples. Deliberately plain ES5-style JS: goja has no
// ES module loader, so `export`/`import`/`require` would be a syntax error.
const EXAMPLE_SINGLE = `// 每次调用返回 1 条消息：计划总条数 = 100 时，generate 会被调用 100 次。
// ctx.index 是「调用序号」(0,1,2…)，不是消息序号。
function generate(ctx) {
  return {
    key: "order-" + ctx.index,
    value: JSON.stringify({
      orderId: "ORD" + String(ctx.index).padStart(5, "0"),
      amount: Math.floor(ctx.random() * 1000) + 1,
      status: ctx.pick(["CREATED", "PAID", "FAILED"]),
      traceId: ctx.uuid(),
      ts: ctx.time
    }),
    headers: { source: "kp-script" }
  };
}`

const EXAMPLE_ARRAY = `// 每次调用返回 2 条（主记录 + 明细）：计划总条数 = 100 时约调用 50 次。
// 第 2 条没写 key，工具自动补 kp-auto-*，分区仍由 key 决定。
// 想提前结束：return null; 想在中途停：返回 null 即可。
function generate(ctx) {
  return [
    {
      key: "order-" + ctx.index,
      value: JSON.stringify({ type: "ORDER", i: ctx.index, ts: ctx.time })
    },
    {
      value: JSON.stringify({
        type: "ITEM",
        order: ctx.index,
        qty: Math.floor(ctx.random() * 5) + 1
      })
    }
  ];
}`

// More runnable templates. Every one of them is valid goja JS and can be
// pasted into the editor / loaded with one click, then checked via Dry-run.
const EXAMPLE_CTX_ALL = `// ctx 全成员示例：index / time / random() / uuid() / pick() 一次用全。
// 顶层变量 seq 跨 generate 调用保留，可用来维护自定义状态。
var seq = 1000;

function generate(ctx) {
  seq += 1;
  var status = ctx.pick(["CREATED", "PAID", "FAILED"]);
  return {
    key: "order-" + ctx.index,
    value: JSON.stringify({
      seq: seq,
      orderId: "ORD" + String(ctx.index).padStart(5, "0"),
      amount: Math.floor(ctx.random() * 1000) + 1,
      status: status,
      traceId: ctx.uuid(),
      ts: ctx.time
    }),
    headers: { source: "kp-script", status: status }
  };
}`

const EXAMPLE_STATE = `// 顶层状态 + 提前结束：ctx.index 到 10 就 return null，流立即停止。
// 计划总条数 100 时，本例只会构造 10 条（结果里 messagesBuilt=10）。
var seq = 1000;

function generate(ctx) {
  if (ctx.index >= 10) return null;
  seq += 1;
  return {
    key: "order-" + ctx.index,
    value: JSON.stringify({ seq: seq, id: ctx.uuid(), ts: ctx.time })
  };
}`

const EXAMPLE_ARRAY_HEADERS = `// 一次返回 2 条（数组）并带 headers；第 2 条不写 key，
// 工具自动补 kp-auto-*，分区仍由 key 决定。
function generate(ctx) {
  var id = ctx.uuid();
  return [
    {
      key: "order-" + ctx.index,
      value: JSON.stringify({ type: "ORDER", id: id, ts: ctx.time }),
      headers: { source: "kp-script", kind: "ORDER" }
    },
    {
      value: JSON.stringify({
        type: "ITEM", id: id,
        qty: Math.floor(ctx.random() * 5) + 1
      }),
      headers: { source: "kp-script", kind: "ITEM" }
    }
  ];
}`

// ---------------------------------------------------------------------------
// ctx API reference. This is the *complete* runtime contract: goja receives
// exactly the five members injected by internal/produce/batch.go:callGenerate.
// Keep this table in sync with that function whenever ctx changes.
// ---------------------------------------------------------------------------
interface CtxMember {
  name: string
  type: string
  desc: string
  usage: string
}

const CTX_MEMBERS: CtxMember[] = [
  {
    name: 'ctx.index',
    type: 'number',
    desc: 'generate 的调用序号，从 0 开始、每次调用 +1。一次返回数组时整批消息共享同一个 index；它是调用序号，不是消息序号。',
    usage: 'key: "order-" + ctx.index',
  },
  {
    name: 'ctx.time',
    type: 'number',
    desc: '服务端当前毫秒时间戳（Unix epoch ms），同一次调用内固定；比 new Date() 更能保证与内核时钟一致。',
    usage: 'ts: ctx.time',
  },
  {
    name: 'ctx.random()',
    type: 'number [0,1)',
    desc: '独立均匀随机浮点数，每次调用重新取样；同一毫秒内连续调用也不会得到相同值。常与 Math.floor 组合成随机整数。',
    usage: 'Math.floor(ctx.random() * 1000) + 1',
  },
  {
    name: 'ctx.uuid()',
    type: 'string',
    desc: '标准 UUID v4 字符串（如 9ba1461d-aa95-43dc-818a-bea27f40b0f9），适合做 traceId、幂等键或唯一 key。',
    usage: 'traceId: ctx.uuid()',
  },
  {
    name: 'ctx.pick(arr)',
    type: 'any',
    desc: '从数组里真随机取一个元素；传对象时随机取一个属性值。空数组 / undefined 返回 undefined（不是按序号轮询）。',
    usage: 'ctx.pick(["CREATED", "PAID", "FAILED"])',
  },
]

const CTX_RECIPES = [
  {
    title: '① 五个成员一起用',
    desc: '随机金额 + 随机状态 + 唯一 ID + 时间戳 + 按序号生成 key，并附 headers。',
    snippet: 'amount: Math.floor(ctx.random()*1000), status: ctx.pick([...])',
    script: EXAMPLE_CTX_ALL,
  },
  {
    title: '② 顶层状态 / 提前结束',
    desc: '脚本顶层变量跨调用保留；返回 null 立即结束，不必凑满计划总条数。',
    snippet: 'var seq = 1000;  if (ctx.index >= 10) return null;',
    script: EXAMPLE_STATE,
  },
  {
    title: '③ 一次多条 / headers',
    desc: '返回非空数组可一次造多条；省略 key 时自动补，分区始终由 key 决定。',
    snippet: 'return [ {key: ..., value: ..., headers: {...}}, {...} ];',
    script: EXAMPLE_ARRAY_HEADERS,
  },
]

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
  const [sHeaders, setSHeaders] = useState<HeaderRow[]>([{ k: '', v: '' }])
  const [sResult, setSResult] = useState<unknown>(null)
  const [sError, setSError] = useState<unknown>(null)
  const [sRunning, setSRunning] = useState(false)

  // --- batch ---
  const [bTopic, setBTopic] = useState('')
  const [bScript, setBScript] = useState(EXAMPLE_SINGLE)
  // Numeric fields are plain text inputs so a large count can be typed or
  // pasted directly (SpinButton only stepped by 1). Keep the raw string in
  // state and derive validated numbers for the request / estimate.
  const [bCountText, setBCountText] = useState('100')
  const [bConcurrencyText, setBConcurrencyText] = useState('8')
  const [bRateText, setBRateText] = useState('0')
  const countValid = /^\d+$/.test(bCountText.trim()) && Number(bCountText) >= 1
  const concurrencyValid =
    /^\d+$/.test(bConcurrencyText.trim()) &&
    Number(bConcurrencyText) >= 1 &&
    Number(bConcurrencyText) <= 32
  const rateValid = /^\d+$/.test(bRateText.trim())
  const bCount = countValid ? Number(bCountText) : 100
  const bConcurrency = concurrencyValid ? Number(bConcurrencyText) : 8
  const bRate = rateValid ? Number(bRateText) : 0
  const formInvalid = !countValid || !concurrencyValid || !rateValid
  const [bResult, setBResult] = useState<BatchResult | null>(null)
  const [bError, setBError] = useState<unknown>(null)
  const [bRunning, setBRunning] = useState(false)
  const [bProgress, setBProgress] = useState<{ produced: number; failed: number } | null>(null)
  const [ctxOpen, setCtxOpen] = useState(() => {
    try {
      return localStorage.getItem('kp:ctxPanelOpen') !== '0'
    } catch {
      return true
    }
  })
  const toggleCtx = useCallback(() => {
    setCtxOpen((v) => {
      const next = !v
      try {
        localStorage.setItem('kp:ctxPanelOpen', next ? '1' : '0')
      } catch {
        /* localStorage unavailable: keep in-memory state only */
      }
      return next
    })
  }, [])
  const jumpToCtx = useCallback(() => {
    if (!ctxOpen) toggleCtx()
    document.getElementById('kp-ctx-panel')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }, [ctxOpen, toggleCtx])

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

  const produceSingle = useCallback(async () => {
    if (!sTopic || !sValue || !sKey.trim()) {
      setSError(new Error('topic、key 和 value 均为必填'))
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
        key: sKey,
        value: sValue,
        headers: Object.keys(headers).length ? headers : undefined,
      })
      setSResult(res)
    } catch (e) {
      setSError(e)
    } finally {
      setSRunning(false)
    }
  }, [sTopic, sKey, sValue, sHeaders])

  const runBatch = useCallback(
    async (dryRun: boolean) => {
      if (!bTopic || !bScript) {
        setBError(new Error('topic 和脚本必填'))
        return
      }
      if (formInvalid) {
        setBError(new Error('计划总条数需 ≥ 1、并发 1~32、速率 ≥ 0，且均为整数'))
        return
      }
      setBRunning(true)
      setBError(null)
      setBResult(null)
      setBProgress(null)
      try {
        const res = await api.produceBatch({
          topic: bTopic,
          script_content: bScript,
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
    [bTopic, bScript, bCount, bConcurrency, bRate, formInvalid],
  )

  const pct =
    bProgress && bCount > 0
      ? Math.min(1, (bProgress.produced + bProgress.failed) / bCount)
      : undefined

  return (
    <div className="kp-fill kp-fill--produce" style={{ gap: 12 }}>
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
              <TopicSelect value={sTopic} onChange={setSTopic} />
            </Field>
            <Field label="Key *" hint="必填；目标分区由 key 自动计算，无需手动指定">
              <Input value={sKey} onChange={(_, d) => setSKey(d.value)} placeholder="消息 key（必填）" />
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
              disabled={sRunning || !sTopic || !sValue || !sKey.trim()}
            >
              {sRunning ? '发送中…' : '发送'}
            </Button>
            {sRunning && <span className="kp-spinner" />}
            <Body1 className="kp-muted" style={{ fontSize: 12 }}>
              目标分区按 key 自动判定
            </Body1>
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
        <Card className="kp-card kp-card--batch">
          <Subtitle2>
            <RocketRegular /> 脚本批量注入
          </Subtitle2>
          <div className={styles.formGrid} style={{ marginTop: 8 }}>
            <Field label="Topic *">
              <TopicSelect value={bTopic} onChange={setBTopic} />
            </Field>
            <Field
              label="计划总条数 (count)"
              hint="可直接键入整数；累计构造到此条数即停止调用 generate"
              validationState={countValid ? 'none' : 'error'}
              validationMessage={countValid ? undefined : '请输入 ≥ 1 的整数'}
            >
              <Input
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                aria-label="计划总条数"
                value={bCountText}
                onChange={(_, d) => setBCountText(d.value)}
              />
            </Field>
            <Field
              label="Concurrency"
              hint="发送并发，1~32"
              validationState={concurrencyValid ? 'none' : 'error'}
              validationMessage={concurrencyValid ? undefined : '请输入 1~32 的整数'}
            >
              <Input
                type="number"
                min={1}
                max={32}
                step={1}
                inputMode="numeric"
                aria-label="发送并发"
                value={bConcurrencyText}
                onChange={(_, d) => setBConcurrencyText(d.value)}
              />
            </Field>
            <Field
              label="Rate (msgs/s, 0=不限)"
              hint="每秒条数上限，0 表示不限速"
              validationState={rateValid ? 'none' : 'error'}
              validationMessage={rateValid ? undefined : '请输入 ≥ 0 的整数'}
            >
              <Input
                type="number"
                min={0}
                step={1}
                inputMode="numeric"
                aria-label="发送速率"
                value={bRateText}
                onChange={(_, d) => setBRateText(d.value)}
              />
            </Field>
          </div>
          <div className="kp-row" style={{ marginTop: 14, alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            <Subtitle2>注入脚本（JavaScript，直接粘贴编辑）</Subtitle2>
            <div className="kp-grow" />
            <Button size="small" onClick={() => setBScript(EXAMPLE_SINGLE)}>示例：每次 1 条</Button>
            <Button size="small" onClick={() => setBScript(EXAMPLE_ARRAY)}>示例：每次 2 条</Button>
            <Button size="small" onClick={() => setBScript(EXAMPLE_CTX_ALL)}>示例：ctx 全成员</Button>
            <Button size="small" appearance="subtle" onClick={() => setBScript('')}>清空</Button>
          </div>
          <textarea
            className="kp-code-editor"
            aria-label="批量注入脚本"
            spellCheck={false}
            value={bScript}
            placeholder={'粘贴 JavaScript，须定义 function generate(ctx) { ... }'}
            onChange={(e) => setBScript(e.target.value)}
          />
          <Body1 className="kp-muted" style={{ fontSize: 12, marginTop: 6 }}>
            直接改上面的代码即可。不要写 export / import / require（goja 沙箱不支持 ES Module 与 IO）；value 必须是字符串，对象请先用 JSON.stringify。{' '}
            <button className="kp-linkbtn" type="button" onClick={jumpToCtx}>
              查看 ctx 全部成员与用法 ↓
            </button>
          </Body1>

          <div className={`${styles.toolbar} kp-batch-actions`}>
            <Button
              icon={<EyeRegular />}
              onClick={() => runBatch(true)}
              disabled={bRunning || formInvalid || !bTopic || !bScript}
            >
              Dry-run 预览
            </Button>
            <Button
              appearance="primary"
              icon={<PlayRegular />}
              onClick={() => runBatch(false)}
              disabled={bRunning || formInvalid || !bTopic || !bScript}
            >
              {bRunning ? '执行中…' : '执行'}
            </Button>
            {bRunning && <span className="kp-spinner" />}
            {formInvalid && (
              <span style={{ fontSize: 12, color: 'var(--kp-danger)' }}>
                请先修正上方数字参数
              </span>
            )}
            {!bTopic && (
              <span style={{ fontSize: 12, color: 'var(--kp-text-tertiary)' }}>
                选择 Topic 后即可 Dry-run / 执行
              </span>
            )}
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
                    调用 generate {bResult.generateCalls ?? 0} 次，构造 {bResult.messagesBuilt ?? 0} 条，展示前 {bResult.samples?.length || 0} 条样例；未实际发送，Kafka 无新消息。
                  </MessageBarBody>
                </MessageBar>
              ) : (
                <MessageBar intent={bResult.failed > 0 ? 'warning' : 'success'}>
                  <MessageBarBody>
                    <MessageBarTitle>完成</MessageBarTitle>
                    调用 generate {bResult.generateCalls ?? 0} 次，构造 {bResult.messagesBuilt ?? 0} 条；成功 {bResult.produced.toLocaleString()}，失败 {bResult.failed.toLocaleString()}，TPS {bResult.tps.toFixed(1)}，耗时 {bResult.elapsedMs} ms
                  </MessageBarBody>
                </MessageBar>
              )}
              <JsonView value={bResult} />
            </div>
          )}
          {!bResult && !bRunning && !bError && (
            <EmptyState
              title="尚未执行"
              hint="上方已填入可运行的示例脚本：选好 Topic 与计划总条数，先点 Dry-run 预览，再执行。"
            />
          )}

          <div className="kp-reference-divider">参考说明 · 放在操作与结果之后，不挡 Dry-run / 执行按钮</div>

          <div className="kp-script-rules">
            <div className="kp-script-rules__title">调用规则 · 一次看懂</div>
            <div className="kp-script-rules__grid">
              <div className="kp-script-rules__item">
                <b>① generate 何时被调用</b>
                <span>Dry-run 调到凑满 5 条样例；正式执行调到累计构造够「计划总条数」；返回 null / undefined、报错或超时则提前停止。</span>
              </div>
              <div className="kp-script-rules__item">
                <b>② 每次应该返回多少</b>
                <span>返回 1 个消息对象 = 1 条；返回数组 = 一次多条；返回 null = 结束。空数组会报错。最常用的是每次 1 条。</span>
              </div>
              <div className="kp-script-rules__item">
                <b>③ 总构造数据量怎么定</b>
                <span>由上方「计划总条数」决定，实际构造 = min(计划总条数, 脚本累计产出)。最后一批超额会自动截断，绝不多写。</span>
              </div>
              <div className="kp-script-rules__item">
                <b>④ ctx 与脚本状态</b>
                <span>ctx.index 按调用次数递增；time / random() / uuid() / pick() 每次刷新；脚本顶层变量跨调用保留；单次调用 5s 超时。</span>
              </div>
            </div>
            <div className="kp-script-rules__formula">
              {countValid ? (
                <>
                  按当前计划总条数 <b>{bCount.toLocaleString()}</b> 估算：每次 1 条 → 调用约 {bCount.toLocaleString()} 次；每次 2 条 → 约 {Math.ceil(bCount / 2).toLocaleString()} 次。返回条数可变时，以结果里的 generateCalls / messagesBuilt 为准。
                </>
              ) : (
                <>「计划总条数」请填 ≥ 1 的整数，填入后这里会实时估算调用次数。</>
              )}
            </div>
          </div>

          <div className="kp-ctx-panel" id="kp-ctx-panel">
            <div className="kp-ctx-panel__head">
              <div className="kp-ctx-panel__heading">
                <div className="kp-ctx-panel__title">ctx 速查 · 全部可用变量与函数</div>
                <div className="kp-ctx-panel__sub">
                  每次调用 <code>generate(ctx)</code> 时内核注入一个全新的 ctx，成员<strong>只有下面 5 个</strong>（2 个变量 + 3 个函数，无其他字段）；脚本顶层变量则跨调用保留。表头可拖拽调整列宽、双击自适应。
                </div>
              </div>
              <div className="kp-ctx-panel__actions">
                <Button size="small" onClick={() => setBScript(EXAMPLE_CTX_ALL)}>载入全成员示例</Button>
                <Button size="small" appearance="subtle" onClick={toggleCtx}>
                  {ctxOpen ? '收起说明' : '展开说明'}
                </Button>
              </div>
            </div>

            {!ctxOpen && (
              <div className="kp-ctx-panel__brief">
                <span>全部成员（共 5 个）：</span>
                {CTX_MEMBERS.map((m) => (
                  <code key={m.name}>{m.name}</code>
                ))}
                <span className="kp-ctx-panel__brief-tip">
                  点「展开说明」查看每个成员的用法、返回值字段与可载入示例；收起状态会被记住。
                </span>
              </div>
            )}

            {ctxOpen && (
              <>
                <table className="kp-ctx-table">
                  <thead>
                    <tr>
                      <th>ctx 成员</th>
                      <th>类型 / 返回</th>
                      <th>作用与边界</th>
                      <th>写法示例</th>
                    </tr>
                  </thead>
                  <tbody>
                    {CTX_MEMBERS.map((m) => (
                      <tr key={m.name}>
                        <td><code className="kp-ctx-code">{m.name}</code></td>
                        <td><span className="kp-ctx-type">{m.type}</span></td>
                        <td>{m.desc}</td>
                        <td><code className="kp-ctx-code">{m.usage}</code></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div className="kp-ctx-panel__foot">
                  ctx 每次调用都重新构造：<code>time</code> 是该次调用的服务端时间，<code>random()</code> / <code>uuid()</code> / <code>pick()</code> 每次重新取样；数组返回时整批共享同一个 <code>index</code>。generate 里不能指定 <code>partition</code>，分区按 key 自动 hash；脚本编译与每次调用各 5s 超时。
                </div>

                <div className="kp-ctx-panel__cols">
                  <div className="kp-ctx-note">
                    <div className="kp-ctx-note__title">返回值的字段规格（每个消息对象）</div>
                    <ul>
                      <li><code>value</code> <b>必填，字符串</b>：消息体；对象要先 <code>JSON.stringify(obj)</code>，否则会变成 <code>[object Object]</code>。</li>
                      <li><code>key</code> 可选，字符串：省略时自动补 <code>kp-auto-*</code>，保证每条都有 key。</li>
                      <li><code>headers</code> 可选，对象：如 <code>{'{ source: "kp-script" }'}</code>，值统一转成字符串。</li>
                      <li>每次可返回 <b>1 个对象</b>、<b>非空数组</b>（一次多条）或 <code>null</code>（结束）；<b>空数组会报错</b>。<code>partition</code> 不生效。</li>
                    </ul>
                  </div>
                  <div className="kp-ctx-note">
                    <div className="kp-ctx-note__title">沙箱里能用什么 / 不能用什么</div>
                    <ul>
                      <li><b>可用</b>：<code>Math</code> <code>JSON</code> <code>Date</code> <code>String</code> <code>Array</code> <code>Object</code> <code>RegExp</code> <code>Map</code> <code>Promise</code>，以及箭头函数、模板字符串、<code>let/const</code>、<code>padStart</code> 等 ES2015+ 语法。</li>
                      <li><b>不可用</b>：<code>console</code> <code>require</code> <code>import</code> <code>export</code> <code>fetch</code> <code>setTimeout</code> <code>process</code>，以及任何文件 / 网络 / 进程 IO；脚本也读不到 Kafka 与 profile 配置。</li>
                      <li>想看脚本产出，请用 <b>Dry-run 预览</b>（返回 5 条样例），不要在脚本里写 <code>console.log</code>。</li>
                    </ul>
                  </div>
                </div>

                <div className="kp-ctx-panel__section-title">怎么用 · 点一下把可运行脚本放进上方编辑器</div>
                <div className="kp-ctx-recipes">
                  {CTX_RECIPES.map((r) => (
                    <div className="kp-ctx-recipe" key={r.title}>
                      <div className="kp-ctx-recipe__title">{r.title}</div>
                      <div className="kp-ctx-recipe__desc">{r.desc}</div>
                      <code className="kp-ctx-recipe__code">{r.snippet}</code>
                      <Button size="small" appearance="subtle" onClick={() => setBScript(r.script)}>
                        载入此脚本
                      </Button>
                    </div>
                  ))}
                </div>
                <div className="kp-ctx-panel__foot">
                  载入后：选 Topic → 设「计划总条数」→ <b>Dry-run 预览</b>（不写 Kafka，看 5 条样例与调用 / 构造计数）→ 确认无误再执行。
                </div>
              </>
            )}
          </div>
        </Card>
      )}
    </div>
  )
}
