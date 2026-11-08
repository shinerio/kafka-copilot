// Reusable form controls:
//  - PortalPanel: dropdown surface rendered into document.body so it is never
//    clipped by a page's `overflow:hidden` container (#5).
//  - TopicSelect: searchable dropdown of existing topics.
//  - PartitionMultiSelect: multi-select dropdown with grid layout + select all.
//  - DateTimeField: native datetime-local picker with seconds, emits RFC3339
//    with a colon-separated timezone offset (+08:00, not +0800).
//  - SelectBox: plain native <select> (avoids Fluent Dropdown popup crashes).

import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react'
import type { CSSProperties, ReactNode, RefObject } from 'react'
import { createPortal } from 'react-dom'
import { Search24Regular, ChevronDownRegular } from '@fluentui/react-icons'
import { api } from '../api'
import type { TopicInfo } from '../types'

// --- PortalPanel -------------------------------------------------------------

interface PanelRect {
  top?: number
  bottom?: number
  left: number
  width: number
  maxHeight: number
}

export function PortalPanel({
  open,
  anchorRef,
  onClose,
  children,
  maxHeight = 340,
  minWidth = 200,
}: {
  open: boolean
  anchorRef: RefObject<HTMLElement | null>
  onClose: () => void
  children: ReactNode
  maxHeight?: number
  minWidth?: number
}) {
  const panelRef = useRef<HTMLDivElement>(null)
  const [rect, setRect] = useState<PanelRect | null>(null)

  const reposition = useCallback(() => {
    const el = anchorRef.current
    if (!el) return
    const r = el.getBoundingClientRect()
    const below = window.innerHeight - r.bottom - 8
    const above = r.top - 8
    const placeBelow = below >= 160 || below >= above
    const avail = placeBelow ? below : above
    setRect({
      top: placeBelow ? r.bottom + 4 : undefined,
      bottom: placeBelow ? undefined : window.innerHeight - r.top + 4,
      left: r.left,
      width: Math.max(r.width, minWidth),
      maxHeight: Math.max(120, Math.min(maxHeight, avail)),
    })
  }, [anchorRef, maxHeight, minWidth])

  useLayoutEffect(() => {
    if (!open) {
      setRect(null)
      return
    }
    reposition()
  }, [open, reposition])

  useEffect(() => {
    if (!open) return
    const onDocDown = (e: MouseEvent) => {
      const t = e.target as Node
      if (panelRef.current?.contains(t)) return
      if (anchorRef.current?.contains(t)) return
      onClose()
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('mousedown', onDocDown)
    document.addEventListener('keydown', onKey)
    window.addEventListener('resize', reposition)
    window.addEventListener('scroll', reposition, true)
    return () => {
      document.removeEventListener('mousedown', onDocDown)
      document.removeEventListener('keydown', onKey)
      window.removeEventListener('resize', reposition)
      window.removeEventListener('scroll', reposition, true)
    }
  }, [open, onClose, anchorRef, reposition])

  if (!open || !rect) return null

  return createPortal(
    <div
      ref={panelRef}
      className="kp-combo__panel kp-combo__panel--portal"
      style={{
        position: 'fixed',
        top: rect.top,
        bottom: rect.bottom,
        left: rect.left,
        width: rect.width,
        maxHeight: rect.maxHeight,
        overflowY: 'auto',
        zIndex: 1000,
      }}
    >
      {children}
    </div>,
    document.body,
  )
}

// --- SelectBox (native, crash-free) ------------------------------------------

export interface SelectOption {
  value: string
  label: string
  disabled?: boolean
}

export function SelectBox({
  value,
  onChange,
  options,
  ariaLabel,
  disabled,
  style,
}: {
  value: string
  onChange: (v: string) => void
  options: SelectOption[]
  ariaLabel?: string
  disabled?: boolean
  style?: CSSProperties
}) {
  return (
    <select
      className="kp-select"
      aria-label={ariaLabel}
      value={value}
      disabled={disabled}
      style={style}
      onChange={(e) => onChange(e.target.value)}
    >
      {options.map((o) => (
        <option key={o.value} value={o.value} disabled={o.disabled}>
          {o.label}
        </option>
      ))}
    </select>
  )
}

// --- TopicSelect -------------------------------------------------------------

export function TopicSelect({
  value,
  onChange,
  label,
}: {
  value: string
  onChange: (v: string) => void
  label?: string
}) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const [topics, setTopics] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const anchorRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    let cancelled = false
    setLoading(true)
    api
      .listTopics('')
      .then((list: TopicInfo[]) => {
        if (cancelled) return
        setTopics((list || []).map((t) => t.name))
        setLoading(false)
      })
      .catch(() => {
        if (!cancelled) {
          setTopics([])
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [open])

  const filtered = topics.filter((t) => t.toLowerCase().includes(query.toLowerCase()))

  return (
    <div style={{ position: 'relative' }} ref={anchorRef}>
      {label && <div className="kp-field-label">{label}</div>}
      <div className="kp-combo" onClick={() => setOpen((o) => !o)} tabIndex={0}>
        <Search24Regular style={{ color: 'var(--kp-text-tertiary)' }} />
        <input
          className="kp-combo__input"
          value={open ? query : value}
          placeholder="选择或搜索 topic…"
          onChange={(e) => {
            setQuery(e.target.value)
            if (!open) setOpen(true)
          }}
          onFocus={() => setOpen(true)}
        />
        <ChevronDownRegular style={{ color: 'var(--kp-text-tertiary)' }} />
      </div>
      <PortalPanel open={open} anchorRef={anchorRef} onClose={() => setOpen(false)}>
        {loading && <div className="kp-combo__item kp-muted">加载中…</div>}
        {!loading && filtered.length === 0 && (
          <div className="kp-combo__item kp-muted">无匹配 topic</div>
        )}
        {filtered.map((t) => (
          <div
            key={t}
            className={'kp-combo__item' + (t === value ? ' kp-combo__item--active' : '')}
            onClick={() => {
              onChange(t)
              setOpen(false)
              setQuery('')
            }}
          >
            {t}
          </div>
        ))}
      </PortalPanel>
    </div>
  )
}

// --- PartitionMultiSelect ----------------------------------------------------

export function PartitionMultiSelect({
  topic,
  value,
  onChange,
  label,
}: {
  topic: string
  value: number[]
  onChange: (v: number[]) => void
  label?: string
}) {
  const [open, setOpen] = useState(false)
  const [parts, setParts] = useState<number[]>([])
  const [loading, setLoading] = useState(false)
  const anchorRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!topic) {
      setParts([])
      return
    }
    let cancelled = false
    setLoading(true)
    api
      .describeTopic(topic)
      .then((t) => {
        if (cancelled) return
        setParts((t.partitions || []).map((p) => p.partition))
        setLoading(false)
      })
      .catch(() => {
        if (!cancelled) {
          setParts([])
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [topic])

  const toggle = (p: number) => {
    if (value.includes(p)) onChange(value.filter((x) => x !== p))
    else onChange([...value, p].sort((a, b) => a - b))
  }

  const summary =
    value.length === 0
      ? '全部分区'
      : value.length <= 4
        ? `已选 ${value.length} 个：${value.join(', ')}`
        : `已选 ${value.length} / ${parts.length} 个分区`

  return (
    <div style={{ position: 'relative' }} ref={anchorRef}>
      {label && <div className="kp-field-label">{label}</div>}
      <div className="kp-combo" onClick={() => setOpen((o) => !o)} tabIndex={0}>
        <input
          className="kp-combo__input"
          value={summary}
          readOnly
          placeholder="选择分区…"
        />
        <ChevronDownRegular style={{ color: 'var(--kp-text-tertiary)' }} />
      </div>
      <PortalPanel
        open={open}
        anchorRef={anchorRef}
        onClose={() => setOpen(false)}
        maxHeight={380}
        minWidth={360}
      >
        {!topic && <div className="kp-combo__item kp-muted">请先选择 topic</div>}
        {topic && loading && <div className="kp-combo__item kp-muted">加载中…</div>}
        {topic && !loading && parts.length === 0 && (
          <div className="kp-combo__item kp-muted">无分区信息</div>
        )}
        {parts.length > 0 && (
          <>
            <div className="kp-combo__tools">
              <span className="kp-muted">共 {parts.length} 个分区</span>
              <div className="kp-grow" />
              <button type="button" className="kp-linkbtn" onClick={() => onChange([...parts])}>
                全选
              </button>
              <button type="button" className="kp-linkbtn" onClick={() => onChange([])}>
                清空
              </button>
            </div>
            <div className="kp-combo__grid">
              {parts.map((p) => (
                <label key={p} className="kp-combo__item kp-combo__check">
                  <input
                    type="checkbox"
                    checked={value.includes(p)}
                    onChange={() => toggle(p)}
                  />
                  <span>P{p}</span>
                </label>
              ))}
            </div>
          </>
        )}
      </PortalPanel>
    </div>
  )
}

// --- DateTimeField -----------------------------------------------------------

function pad(n: number): string {
  return String(n).padStart(2, '0')
}

// RFC3339 (or any Date-parseable ISO) -> local "YYYY-MM-DDTHH:mm:ss".
function rfc3339ToLocalInput(iso: string): string {
  if (!iso) return ''
  const d = new Date(iso)
  if (isNaN(d.getTime())) return ''
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
  )
}

// Local "YYYY-MM-DDTHH:mm:ss" -> RFC3339 with a colon in the offset:
// "2026-11-07T19:23:00+08:00" (Go's time.RFC3339 requires the colon).
function localInputToRFC3339(local: string): string {
  if (!local) return ''
  const d = new Date(local)
  if (isNaN(d.getTime())) return ''
  const off = -d.getTimezoneOffset()
  const sign = off >= 0 ? '+' : '-'
  const abs = Math.abs(off)
  const tz = `${sign}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}${tz}`
  )
}

export function DateTimeField({
  label,
  value,
  onChange,
}: {
  label: string
  value: string
  onChange: (iso: string) => void
}) {
  const [local, setLocal] = useState<string>(() => rfc3339ToLocalInput(value))

  useEffect(() => {
    setLocal(rfc3339ToLocalInput(value))
  }, [value])

  return (
    <div>
      <div className="kp-field-label">{label}</div>
      <div className="kp-datetime-row">
        <input
          type="datetime-local"
          step={1}
          className="kp-datetime"
          value={local}
          onChange={(e) => {
            setLocal(e.target.value)
            onChange(localInputToRFC3339(e.target.value))
          }}
        />
        <button
          type="button"
          className="kp-linkbtn"
          title="清除时间"
          onClick={() => {
            setLocal('')
            onChange('')
          }}
        >
          清空
        </button>
      </div>
    </div>
  )
}
