// Settings page (FR-13 + FR-16): profile CRUD with SASL/TLS fields,
// configurable data directory (kp-data) and audit retention.

import {
  Title3,
  Body1,
  Subtitle2,
  Input,
  Field,
  Button,
  Switch,
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
  MessageBar,
  MessageBarBody,
  MessageBarTitle,
} from '@fluentui/react-components'
import {
  AddRegular,
  SaveRegular,
  DeleteRegular,
  StarRegular,
  EraserRegular,
} from '@fluentui/react-icons'
import { useCallback, useEffect, useState } from 'react'
import { api } from '../api'
import type { Profile, ProfileInput, SASL, TLS, SettingsInfo } from '../types'
import { EmptyState, ErrorState, JsonView } from '../components/ui'
import { SelectBox } from '../components/controls'

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
    flexWrap: 'wrap',
  },
  tableWrap: {
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    borderRadius: '8px',
    overflow: 'hidden',
    background: tokens.colorNeutralBackground1,
  },
})

interface FormState {
  name: string
  bootstrap: string
  saslEnabled: boolean
  saslMechanism: string
  saslUsername: string
  saslPassword: string
  tlsEnabled: boolean
  tlsCaCert: string
  tlsSkipVerify: boolean
}

const EMPTY: FormState = {
  name: '',
  bootstrap: '',
  saslEnabled: false,
  saslMechanism: 'plain',
  saslUsername: '',
  saslPassword: '',
  tlsEnabled: false,
  tlsCaCert: '',
  tlsSkipVerify: false,
}

function toForm(p: Profile): FormState {
  return {
    name: p.name,
    bootstrap: (p.bootstrap_servers || p.bootstrapServers || []).join(', '),
    saslEnabled: !!p.sasl,
    saslMechanism: p.sasl?.mechanism || 'plain',
    saslUsername: p.sasl?.username || '',
    saslPassword: p.sasl?.password || '',
    tlsEnabled: !!p.tls,
    tlsCaCert: p.tls?.ca_cert || p.tls?.caCert || '',
    tlsSkipVerify: p.tls?.skip_verify ?? p.tls?.skipVerify ?? false,
  }
}

function toInput(f: FormState): ProfileInput {
  const servers = f.bootstrap
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s !== '')
  let sasl: SASL | null = null
  if (f.saslEnabled && f.saslMechanism) {
    sasl = {
      mechanism: f.saslMechanism,
      username: f.saslUsername,
      password: f.saslPassword,
    }
  }
  let tls: TLS | null = null
  if (f.tlsEnabled) {
    tls = { ca_cert: f.tlsCaCert, skip_verify: f.tlsSkipVerify }
  }
  return { name: f.name, bootstrap_servers: servers, sasl, tls }
}

export function SettingsPage({
  profiles,
  activeProfile,
  onChanged,
}: {
  profiles: Profile[]
  activeProfile: string
  onChanged: () => void
}) {
  const styles = useStyles()
  const [form, setForm] = useState<FormState>(EMPTY)
  const [editing, setEditing] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState<{ kind: 'ok' | 'error'; text: string } | null>(null)
  const [auditBefore, setAuditBefore] = useState('')
  const [auditMsg, setAuditMsg] = useState<string | null>(null)
  const [auditError, setAuditError] = useState<unknown>(null)

  // --- Configurable data directory (kp-data) ---
  const [layout, setLayout] = useState<SettingsInfo | null>(null)
  const [dirInput, setDirInput] = useState('')
  const [copyCurrent, setCopyCurrent] = useState(true)
  const [dirBusy, setDirBusy] = useState(false)
  const [dirMsg, setDirMsg] = useState<{ kind: 'ok' | 'error'; text: string } | null>(null)
  const [dirError, setDirError] = useState<unknown>(null)

  const loadLayout = useCallback(async () => {
    try {
      const info = await api.getSettings()
      setLayout(info)
      setDirInput(info.data_dir)
      setDirError(null)
    } catch (e) {
      setDirError(e)
    }
  }, [])

  useEffect(() => {
    void loadLayout()
  }, [loadLayout])

  useEffect(() => {
    if (dirMsg) {
      const t = window.setTimeout(() => setDirMsg(null), 6000)
      return () => window.clearTimeout(t)
    }
  }, [dirMsg])

  const saveDataDir = useCallback(async () => {
    const value = dirInput.trim()
    if (!value) {
      setDirMsg({ kind: 'error', text: '数据目录不能为空' })
      return
    }
    setDirBusy(true)
    setDirMsg(null)
    try {
      const res = await api.updateSettings({ data_dir: value, copy_current: copyCurrent })
      const moved = res.copied.length
        ? `已复制 ${res.copied.join('、')}`
        : res.skipped.length
          ? `目标已存在 ${res.skipped.join('、')}（未覆盖）`
          : '没有可复制的数据'
      setDirMsg({
        kind: 'ok',
        text: `${res.status === 'unchanged' ? '目录未变化' : `已写入 ${res.settings_path}`}，${moved}。请执行 kp restart 后生效。`,
      })
      await loadLayout()
    } catch (e) {
      setDirMsg({ kind: 'error', text: e instanceof Error ? e.message : String(e) })
    } finally {
      setDirBusy(false)
    }
  }, [dirInput, copyCurrent, loadLayout])

  const migrateData = useCallback(async () => {
    setDirBusy(true)
    setDirMsg(null)
    try {
      const res = await api.migrateSettings(false)
      if (res.already_done) {
        setDirMsg({ kind: 'ok', text: `无需重复迁移：${res.note || '已完成一次性迁移'}` })
      } else if (res.copied.length === 0) {
        setDirMsg({ kind: 'ok', text: '旧目录没有可迁移的数据，或目标已存在同名文件（未覆盖）。' })
      } else {
        setDirMsg({
          kind: 'ok',
          text: `已从 ${res.source} 迁移 ${res.copied.join('、')} 到 ${res.dest}；原文件保留以便回滚，重启服务后生效。`,
        })
      }
      await loadLayout()
    } catch (e) {
      setDirMsg({ kind: 'error', text: e instanceof Error ? e.message : String(e) })
    } finally {
      setDirBusy(false)
    }
  }, [loadLayout])

  useEffect(() => {
    if (msg) {
      const t = window.setTimeout(() => setMsg(null), 4000)
      return () => window.clearTimeout(t)
    }
  }, [msg])

  const startAdd = useCallback(() => {
    setForm(EMPTY)
    setEditing(null)
  }, [])

  const startEdit = useCallback(
    (name: string) => {
      const p = profiles.find((x) => x.name === name)
      if (p) {
        setForm(toForm(p))
        setEditing(name)
      }
    },
    [profiles],
  )

  const save = useCallback(async () => {
    if (!form.name || !form.bootstrap) {
      setMsg({ kind: 'error', text: 'name 和 bootstrap servers 必填' })
      return
    }
    setBusy(true)
    setMsg(null)
    try {
      const input = toInput(form)
      if (editing) {
        await api.updateProfile(editing, input)
        setMsg({ kind: 'ok', text: `已更新 profile "${editing}"` })
      } else {
        await api.createProfile(input)
        setMsg({ kind: 'ok', text: `已新增 profile "${form.name}"` })
      }
      onChanged()
    } catch (e) {
      setMsg({ kind: 'error', text: e instanceof Error ? e.message : String(e) })
    } finally {
      setBusy(false)
    }
  }, [form, editing, onChanged])

  const remove = useCallback(
    async (name: string) => {
      if (!confirm(`确认删除 profile "${name}"？`)) return
      setBusy(true)
      try {
        await api.deleteProfile(name)
        setMsg({ kind: 'ok', text: `已删除 "${name}"` })
        if (editing === name) {
          setForm(EMPTY)
          setEditing(null)
        }
        onChanged()
      } catch (e) {
        setMsg({ kind: 'error', text: e instanceof Error ? e.message : String(e) })
      } finally {
        setBusy(false)
      }
    },
    [editing, onChanged],
  )

  const setDefault = useCallback(
    async (name: string) => {
      setBusy(true)
      try {
        await api.setDefaultProfile(name)
        setMsg({ kind: 'ok', text: `已设 "${name}" 为默认` })
        onChanged()
      } catch (e) {
        setMsg({ kind: 'error', text: e instanceof Error ? e.message : String(e) })
      } finally {
        setBusy(false)
      }
    },
    [onChanged],
  )

  const clearAudit = useCallback(async () => {
    setAuditError(null)
    setAuditMsg(null)
    try {
      // datetime-local -> RFC3339
      let before: string | undefined
      if (auditBefore) {
        const d = new Date(auditBefore)
        before = d.toISOString()
      }
      await api.clearAudit(before)
      setAuditMsg(before ? `已清除 ${before} 之前的审计数据` : '已清除全部审计数据')
    } catch (e) {
      setAuditError(e)
    }
  }, [auditBefore])

  const set = <K extends keyof FormState>(k: K, v: FormState[K]) =>
    setForm((prev) => ({ ...prev, [k]: v }))

  return (
    <div className="kp-fill kp-fill--settings" style={{ gap: 12 }}>
      <Title3>设置</Title3>

      {/* Configurable data directory */}
      <Card className="kp-card">
        <div className="kp-row" style={{ justifyContent: 'space-between' }}>
          <Subtitle2>数据目录（Profile / 审计 / 缓存）</Subtitle2>
          {layout?.pending_change && (
            <span className="kp-pill kp-pill--running" style={{ marginLeft: 8 }}>
              待重启生效
            </span>
          )}
        </div>
        <Body1 className="kp-muted" style={{ marginTop: 4 }}>
          默认是 kp.exe 同目录的 kp-data；设置文件 kp-settings.yaml 固定放在 kp.exe 旁。
          修改数据目录后需执行 kp restart 才会切换。
        </Body1>

        {layout && (
          <div className="kp-path-grid">
            <span className="kp-muted">设置文件</span>
            <span className="kp-cell-mono">{layout.settings_path}</span>
            <span className="kp-muted">当前生效</span>
            <span className="kp-cell-mono">{layout.active_data_dir || layout.data_dir}</span>
            <span className="kp-muted">配置目录</span>
            <span className="kp-cell-mono">{layout.data_dir}</span>
            <span className="kp-muted">默认目录</span>
            <span className="kp-cell-mono">{layout.default_data_dir}</span>
            <span className="kp-muted">旧版目录</span>
            <span className="kp-cell-mono">
              {layout.legacy_dir}
              {layout.legacy_config_exists || layout.legacy_audit_exists ? '（仍有数据）' : '（无数据）'}
            </span>
            <span className="kp-muted">Profile 文件</span>
            <span className="kp-cell-mono">
              {layout.config_path}{layout.config_exists ? '' : '（不存在）'}
            </span>
            <span className="kp-muted">审计库</span>
            <span className="kp-cell-mono">
              {layout.audit_path}{layout.audit_exists ? '' : '（不存在）'}
            </span>
            <span className="kp-muted">迁移状态</span>
            <span className="kp-cell-mono">
              {layout.migration_done ? `已完成（${layout.migrated_at || '时间未记录'}）` : '尚未迁移'}
              {layout.env_override ? ` · KP_APPDATA=${layout.env_override}` : ''}
            </span>
          </div>
        )}
        {Boolean(dirError) && <ErrorState error={dirError} />}

        <div style={{ marginTop: 12 }}>
          <Field label="数据目录（相对路径基于 kp.exe 所在目录）">
            <Input
              value={dirInput}
              onChange={(_, d) => setDirInput(d.value)}
              placeholder="kp-data"
            />
          </Field>
        </div>
        <div className="kp-row" style={{ marginTop: 8, flexWrap: 'wrap' }}>
          <Switch
            checked={copyCurrent}
            onChange={(_, d) => setCopyCurrent(d.checked)}
            label="同时复制现有数据"
          />
          <Button
            appearance="primary"
            icon={<SaveRegular />}
            onClick={saveDataDir}
            disabled={dirBusy}
          >
            保存目录
          </Button>
          <Button
            appearance="subtle"
            onClick={() => layout && setDirInput(layout.default_data_dir)}
            disabled={dirBusy}
          >
            恢复默认值
          </Button>
          <Button
            appearance="subtle"
            icon={<EraserRegular />}
            onClick={migrateData}
            disabled={dirBusy}
          >
            从旧位置迁移
          </Button>
          {dirBusy && <span className="kp-spinner" />}
        </div>
        <Body1 className="kp-muted" style={{ marginTop: 8 }}>
          切换目录前建议先 kp stop，避免审计库 WAL 复制时少掉最后几条记录；源文件始终保留、绝不删除或覆盖。
          旧版目录的一次性迁移只执行一次。
        </Body1>
        {dirMsg && (
          <MessageBar
            intent={dirMsg.kind === 'ok' ? 'success' : 'error'}
            style={{ marginTop: 8 }}
          >
            <MessageBarBody>
              <MessageBarTitle>{dirMsg.kind === 'ok' ? '成功' : '失败'}</MessageBarTitle>
              {dirMsg.text}
            </MessageBarBody>
          </MessageBar>
        )}
      </Card>

      {/* Profile list */}
      <Card className="kp-card">
        <div className="kp-row" style={{ justifyContent: 'space-between' }}>
          <Subtitle2>Profile 管理</Subtitle2>
          <Button size="small" icon={<AddRegular />} appearance="subtle" onClick={startAdd}>
            新增
          </Button>
        </div>
        <div className={styles.tableWrap} style={{ marginTop: 8 }}>
          <Table size="small">
            <TableHeader>
              <TableRow>
                <TableHeaderCell>Name</TableHeaderCell>
                <TableHeaderCell>Bootstrap Servers</TableHeaderCell>
                <TableHeaderCell>SASL</TableHeaderCell>
                <TableHeaderCell>TLS</TableHeaderCell>
                <TableHeaderCell>操作</TableHeaderCell>
              </TableRow>
            </TableHeader>
            <TableBody>
              {profiles.length === 0 && (
                <TableRow>
                  <TableCell colSpan={5}>
                    <EmptyState title="尚无 profile" hint="使用下方表单新增一个连接。" />
                  </TableCell>
                </TableRow>
              )}
              {profiles.map((p) => (
                <TableRow key={p.name}>
                  <TableCell className="kp-cell-mono">
                    {p.name}
                    {p.name === activeProfile && (
                      <span className="kp-pill kp-pill--ok" style={{ marginLeft: 6 }}>
                        默认
                      </span>
                    )}
                  </TableCell>
                  <TableCell className="kp-cell-mono">
                    {(p.bootstrap_servers || p.bootstrapServers || []).join(', ')}
                  </TableCell>
                  <TableCell>{p.sasl ? p.sasl.mechanism : '—'}</TableCell>
                  <TableCell>{p.tls ? 'on' : '—'}</TableCell>
                  <TableCell>
                    <div className="kp-row">
                      <Button size="small" appearance="subtle" onClick={() => startEdit(p.name)}>
                        编辑
                      </Button>
                      <Button
                        size="small"
                        appearance="subtle"
                        icon={<StarRegular />}
                        onClick={() => setDefault(p.name)}
                        disabled={p.name === activeProfile}
                      >
                        设默认
                      </Button>
                      <Button
                        size="small"
                        appearance="subtle"
                        icon={<DeleteRegular />}
                        onClick={() => remove(p.name)}
                      >
                        删除
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </Card>

      {/* Add / edit form */}
      <Card className="kp-card">
        <Subtitle2>{editing ? `编辑 "${editing}"` : '新增 Profile'}</Subtitle2>
        <div className={styles.formGrid} style={{ marginTop: 8 }}>
          <Field label="Name *">
            <Input
              value={form.name}
              onChange={(_, d) => set('name', d.value)}
              disabled={!!editing}
            />
          </Field>
          <Field label="Bootstrap Servers * (逗号分隔)">
            <Input
              placeholder="broker1:9092, broker2:9092"
              value={form.bootstrap}
              onChange={(_, d) => set('bootstrap', d.value)}
            />
          </Field>
        </div>

        <Divider style={{ margin: '12px 0' }} />

        <div className="kp-row">
          <Switch
            checked={form.saslEnabled}
            onChange={(_, d) => set('saslEnabled', d.checked)}
            label="启用 SASL"
          />
          <Switch
            checked={form.tlsEnabled}
            onChange={(_, d) => set('tlsEnabled', d.checked)}
            label="启用 TLS"
          />
        </div>

        {form.saslEnabled && (
          <div className={styles.formGrid} style={{ marginTop: 8 }}>
            <Field label="SASL Mechanism">
              {/* Native select: Fluent Dropdown popup crashed the page on click. */}
              <SelectBox
                ariaLabel="SASL Mechanism"
                value={form.saslMechanism}
                onChange={(v) => set('saslMechanism', v)}
                options={[
                  { value: 'plain', label: 'plain' },
                  { value: 'scram-sha-256', label: 'scram-sha-256' },
                  { value: 'scram-sha-512', label: 'scram-sha-512' },
                ]}
              />
            </Field>
            <Field label="Username">
              <Input value={form.saslUsername} onChange={(_, d) => set('saslUsername', d.value)} />
            </Field>
            <Field label="Password">
              <Input
                type="password"
                value={form.saslPassword}
                onChange={(_, d) => set('saslPassword', d.value)}
                placeholder={editing ? '****（留空保持不变）' : ''}
              />
            </Field>
          </div>
        )}

        {form.tlsEnabled && (
          <div className={styles.formGrid} style={{ marginTop: 8 }}>
            <Field label="CA Cert (PEM 文件路径)">
              <Input value={form.tlsCaCert} onChange={(_, d) => set('tlsCaCert', d.value)} />
            </Field>
            <Field label="Skip Verify">
              <Switch
                checked={form.tlsSkipVerify}
                onChange={(_, d) => set('tlsSkipVerify', d.checked)}
                label={form.tlsSkipVerify ? '跳过校验' : '校验服务器证书'}
              />
            </Field>
          </div>
        )}

        <div className={styles.toolbar}>
          <Button
            appearance="primary"
            icon={<SaveRegular />}
            onClick={save}
            disabled={busy}
          >
            {editing ? '保存修改' : '新增'}
          </Button>
          {busy && <span className="kp-spinner" />}
          {editing && (
            <Button appearance="subtle" onClick={startAdd}>
              取消编辑
            </Button>
          )}
        </div>

        {msg && (
          <MessageBar
            intent={msg.kind === 'ok' ? 'success' : 'error'}
            style={{ marginTop: 8 }}
          >
            <MessageBarBody>
              <MessageBarTitle>{msg.kind === 'ok' ? '成功' : '失败'}</MessageBarTitle>
              {msg.text}
            </MessageBarBody>
          </MessageBar>
        )}
      </Card>

      {/* Audit retention */}
      <Card className="kp-card">
        <Subtitle2>审计保留</Subtitle2>
        <Body1 className="kp-muted" style={{ marginTop: 4 }}>
          清除指定时间之前的审计数据（留空则清除全部）。
        </Body1>
        <div className="kp-row" style={{ marginTop: 8 }}>
          <Field label="清除该时间之前">
            <input
              type="datetime-local"
              value={auditBefore}
              onChange={(e) => setAuditBefore(e.target.value)}
              style={{
                padding: '6px 8px',
                border: `1px solid ${tokens.colorNeutralStroke1}`,
                borderRadius: '4px',
                fontFamily: 'inherit',
                fontSize: '14px',
              }}
            />
          </Field>
          <Button icon={<EraserRegular />} onClick={clearAudit} disabled={busy}>
            清除
          </Button>
        </div>
        {Boolean(auditError) && <ErrorState error={auditError} />}
        {auditMsg && (
          <MessageBar intent="success" style={{ marginTop: 8 }}>
            <MessageBarBody>{auditMsg}</MessageBarBody>
          </MessageBar>
        )}
      </Card>

      {/* Live preview of the input payload (debug aid) */}
      <Card className="kp-card">
        <Subtitle2>当前表单负载预览</Subtitle2>
        <JsonView value={toInput(form)} maxHeight={200} />
      </Card>
    </div>
  )
}
