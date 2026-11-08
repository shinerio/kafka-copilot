// Global, page-scoped column resizing for every table in the app.
//
// Fluent UI's Table has no built-in resize handle, so we attach a thin grip to
// each <th> after render. Widths are persisted per page + table + column in
// localStorage. Drag to resize; double-click the grip to auto-fit the column
// to its content. A MutationObserver re-attaches grips when React re-renders
// a table (e.g. loading -> data), so all pages are covered without touching
// each individual table component.

import { useEffect } from 'react'

const MIN_WIDTH = 56
const MAX_WIDTH = 900
const clamp = (n: number) => Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, Math.round(n)))

export function useResizableTables(pageKey: string) {
  useEffect(() => {
    const root = document.querySelector('main') || document.body
    let frame = 0
    let observer: MutationObserver | null = null

    const storageKey = (tableId: string, col: number) =>
      `kp:colw:${pageKey}:${tableId}:${col}`

    const widthOf = (el: HTMLElement) => el.getBoundingClientRect().width

    // Switching a table to fixed layout needs explicit widths on every header,
    // otherwise the browser redistributes the remaining space.
    const enableFixed = (table: HTMLTableElement) => {
      table.style.tableLayout = 'fixed'
      const headers = Array.from(table.querySelectorAll('thead th')) as HTMLElement[]
      headers.forEach((th) => {
        if (!th.style.width) th.style.width = `${Math.round(widthOf(th))}px`
      })
    }

    const startDrag = (
      e: MouseEvent,
      table: HTMLTableElement,
      th: HTMLElement,
      tableId: string,
      col: number,
    ) => {
      e.preventDefault()
      e.stopPropagation()
      enableFixed(table)
      const startX = e.clientX
      const startW = widthOf(th)
      document.body.classList.add('kp-col-resizing')

      const onMove = (ev: MouseEvent) => {
        th.style.width = `${clamp(startW + ev.clientX - startX)}px`
      }
      const onUp = (ev: MouseEvent) => {
        document.removeEventListener('mousemove', onMove)
        document.removeEventListener('mouseup', onUp)
        document.body.classList.remove('kp-col-resizing')
        const w = clamp(startW + ev.clientX - startX)
        th.style.width = `${w}px`
        localStorage.setItem(storageKey(tableId, col), String(w))
      }
      document.addEventListener('mousemove', onMove)
      document.addEventListener('mouseup', onUp)
    }

    const autoFit = (
      table: HTMLTableElement,
      th: HTMLElement,
      col: number,
      tableId: string,
    ) => {
      enableFixed(table)
      let max = widthOf(th)
      const rows = Array.from(table.querySelectorAll('tbody tr'))
      for (const tr of rows) {
        const td = tr.children[col] as HTMLElement | undefined
        if (td) max = Math.max(max, td.scrollWidth + 24)
      }
      const w = clamp(max)
      th.style.width = `${w}px`
      localStorage.setItem(storageKey(tableId, col), String(w))
    }

    const enhance = () => {
      const tables = Array.from(root.querySelectorAll('table'))
      tables.forEach((table, ti) => {
        const headers = Array.from(table.querySelectorAll('thead th')) as HTMLElement[]
        if (headers.length === 0) return
        const tableId = (table.querySelector('thead')?.textContent || `table${ti}`)
          .replace(/\s+/g, ' ')
          .trim()
          .slice(0, 100)

        let hasSaved = false
        headers.forEach((th, ci) => {
          const saved = localStorage.getItem(storageKey(tableId, ci))
          if (saved) {
            th.style.width = `${Number(saved)}px`
            hasSaved = true
          }
        })
        if (hasSaved) table.style.tableLayout = 'fixed'

        headers.forEach((th, ci) => {
          th.style.position = 'relative'
          if (th.querySelector(':scope > [data-kp-col-resizer]')) return
          const grip = document.createElement('span')
          grip.className = 'kp-col-resizer'
          grip.dataset.kpColResizer = '1'
          grip.title = '拖拽调整列宽；双击按内容自适应'
          grip.addEventListener('mousedown', (e) => startDrag(e, table, th, tableId, ci))
          grip.addEventListener('dblclick', (e) => {
            e.preventDefault()
            e.stopPropagation()
            autoFit(table, th, ci, tableId)
          })
          th.appendChild(grip)
        })
      })
    }

    const schedule = () => {
      if (frame) return
      frame = requestAnimationFrame(() => {
        frame = 0
        enhance()
      })
    }

    enhance()
    observer = new MutationObserver(schedule)
    observer.observe(root, { childList: true, subtree: true })

    return () => {
      observer?.disconnect()
      if (frame) cancelAnimationFrame(frame)
      root.querySelectorAll('[data-kp-col-resizer]').forEach((n) => n.remove())
      document.body.classList.remove('kp-col-resizing')
    }
  }, [pageKey])
}
