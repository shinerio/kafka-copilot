// Kafka Pilot Web UI root.
// Orchestrates global state (profiles, active profile, backend connection
// status) and switches between page components. Theme comes from Fluent UI
// v9 webLightTheme; per-page details live in src/pages/*.

import {
  FluentProvider,
  webDarkTheme,
  Toaster,
  makeStyles,
  type Theme,
} from '@fluentui/react-components'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { Layout } from './components/Layout'
import { ErrorBoundary } from './components/ErrorBoundary'
import { useResizableTables } from './components/resizableTables'
import type { PageKey } from './components/Layout'
import { api } from './api'
import type { Profile } from './types'
import { TopicsPage } from './pages/Topics'
import { ScanPage } from './pages/Scan'
import { ProducePage } from './pages/Produce'
import { GroupsPage } from './pages/Groups'
import { ObservePage } from './pages/Observe'
import { SettingsPage } from './pages/Settings'

const useStyles = makeStyles({
  root: {
    height: '100%',
    width: '100%',
  },
})

// Dev proxy target port, mirrors vite.config.ts -> 127.0.0.1:18080.
// In production build the Go server serves both UI and /api on the same port,
// so we display the dev proxy port only as an informational hint.
const DEV_PROXY_PORT = 18080

// 工业控制台暗色主题：Fluent 中性色对齐 index.css 的 --kp-* 暗色调色板，
// 品牌色改为琥珀（对齐 --kp-accent），外壳（顶栏/侧栏/卡片）随主题自动变暗。
const kpTheme: Theme = {
  ...webDarkTheme,
  colorNeutralBackground1: '#151b24',
  colorNeutralBackground2: '#0d1117',
  colorNeutralBackground3: '#1b232e',
  colorNeutralBackground4: '#222a34',
  colorNeutralBackgroundInverted: '#e7ebf1',
  colorNeutralForeground1: '#e7ebf1',
  colorNeutralForeground2: '#9aa4b2',
  colorNeutralForeground3: '#6b7480',
  colorNeutralForeground4: '#5c6470',
  colorNeutralStroke1: 'rgba(255,255,255,0.09)',
  colorNeutralStroke2: 'rgba(255,255,255,0.08)',
  colorNeutralStrokeAccessible: 'rgba(255,255,255,0.18)',
  colorBrandBackground: '#ff9f43',
  colorBrandBackgroundHover: '#ffb362',
  colorBrandBackgroundPressed: '#ffa64d',
  colorBrandForeground1: '#ff9f43',
  colorBrandForeground2: '#ffb362',
  colorBrandForeground2Hover: '#ffc07d',
  colorBrandForeground2Pressed: '#ffb362',
  colorBrandBackground2: 'rgba(255,159,67,0.16)',
  colorBrandBackground2Hover: 'rgba(255,159,67,0.24)',
  colorBrandBackground2Pressed: 'rgba(255,159,67,0.24)',
  // 圆角令牌：让所有 Fluent 控件（输入框 / 按钮 / 卡片）统一变圆滑高级
  borderRadiusSmall: '8px',
  borderRadiusMedium: '10px',
  borderRadiusLarge: '14px',
  borderRadiusXLarge: '18px',
  borderRadiusCircular: '999px',
  // 聚焦描边改为琥珀，跨所有 Fluent 控件统一焦点光环
  colorStrokeFocus1: '#ff9f43',
  colorCompoundBrandStroke: 'rgba(255,159,67,0.55)',
}

export default function App() {
  const styles = useStyles()
  const [page, setPage] = useState<PageKey>('topics')
  // Every table on the current page gets drag-to-resize / double-click auto-fit.
  useResizableTables(page)
  const [profiles, setProfiles] = useState<Profile[]>([])
  const [activeProfile, setActiveProfile] = useState('')
  const [connected, setConnected] = useState(false)
  const [actor] = useState(() => {
    // Mirrors internal/app.GetActor()
    return (
      (typeof localStorage !== 'undefined' && localStorage.getItem('kp:actor')) ||
      'unknown'
    )
  })

  // Load profiles on mount.
  const refreshProfiles = useCallback(async () => {
    try {
      const list = await api.listProfiles()
      setProfiles(list)
      // Pick a default: previously chosen, else first profile.
      const saved = localStorage.getItem('kp:activeProfile') || ''
      if (saved && list.some((p) => p.name === saved)) {
        setActiveProfile(saved)
      } else if (list.length > 0) {
        setActiveProfile(list[0].name)
      } else {
        setActiveProfile('')
      }
    } catch {
      setProfiles([])
    }
  }, [])

  useEffect(() => {
    void refreshProfiles()
  }, [refreshProfiles])

  // Health check for the connection indicator.
  useEffect(() => {
    let cancelled = false
    const check = async () => {
      try {
        await api.health()
        if (!cancelled) setConnected(true)
      } catch {
        if (!cancelled) setConnected(false)
      }
    }
    void check()
    const id = window.setInterval(check, 5000)
    return () => {
      cancelled = true
      window.clearInterval(id)
    }
  }, [])

  const handleSelectProfile = useCallback((name: string) => {
    setActiveProfile(name)
    localStorage.setItem('kp:activeProfile', name)
  }, [])

  const pageEl = useMemo(() => {
    switch (page) {
      case 'topics':
        return <TopicsPage profile={activeProfile} />
      case 'scan':
        return <ScanPage profile={activeProfile} />
      case 'produce':
        return <ProducePage profile={activeProfile} />
      case 'groups':
        return <GroupsPage profile={activeProfile} />
      case 'observe':
        return <ObservePage />
      case 'settings':
        return (
          <SettingsPage
            profiles={profiles}
            activeProfile={activeProfile}
            onChanged={refreshProfiles}
          />
        )
    }
  }, [page, activeProfile, profiles, refreshProfiles])

  return (
    <FluentProvider theme={kpTheme} className={styles.root}>
      <ErrorBoundary label="应用外壳">
        <Layout
          page={page}
          onNavigate={setPage}
          profiles={profiles}
          activeProfile={activeProfile}
          onSelectProfile={handleSelectProfile}
          connected={connected}
          port={connected ? DEV_PROXY_PORT : null}
          actor={actor}
        >
          <ErrorBoundary label={page}>{pageEl}</ErrorBoundary>
        </Layout>
      </ErrorBoundary>
      <Toaster />
    </FluentProvider>
  )
}
