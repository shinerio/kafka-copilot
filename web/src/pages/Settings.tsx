// Settings page (FR-13 + audit retention): profile CRUD with SASL/TLS fields
// and audit data clear (before a given timestamp).

import {
  Title3,
  Body1,
  Subtitle2,
  Input,
  Field,
  Button,
  Dropdown,
  Option,
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
import type { Profile, ProfileInput, SASL, TLS } from '../types'
import { EmptyState, ErrorState, JsonView } from '../components/ui'

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
    <div className="kp-fill" style={{ gap: 12 }}>
      <Title3>设置</Title3>

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
              <Dropdown
                value={form.saslMechanism}
                selectedOptions={[form.saslMechanism]}
                onOptionSelect={(_, d) => set('saslMechanism', String(d.optionValue))}
              >
                <Option value="plain">plain</Option>
                <Option value="scram-sha-256">scram-sha-256</Option>
                <Option value="scram-sha-512">scram-sha-512</Option>
              </Dropdown>
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
