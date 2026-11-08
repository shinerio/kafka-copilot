// Kafka Pilot Web UI root.
// Orchestrates global state (profiles, active profile, backend connection
// status) and switches between page components. Theme comes from Fluent UI
// v9 webLightTheme; per-page details live in src/pages/*.

import { FluentProvider, webLightTheme, Toaster, makeStyles } from '@fluentui/react-components'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { Layout } from './components/Layout'
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

export default function App() {
  const styles = useStyles()
  const [page, setPage] = useState<PageKey>('topics')
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
    <FluentProvider theme={webLightTheme} className={styles.root}>
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
        {pageEl}
      </Layout>
      <Toaster />
    </FluentProvider>
  )
}
