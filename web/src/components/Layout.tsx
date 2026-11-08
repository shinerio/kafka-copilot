// Application shell: top bar (profile picker + connection status + port),
// side navigation, and a scrollable content region. Purely presentational;
// state lives in App.tsx.

import {
  Toolbar,
  ToolbarButton,
  Tooltip,
  Divider,
  makeStyles,
  tokens,
  Subtitle1,
  Body1,
} from '@fluentui/react-components'
import {
  DataUsageRegular,
  RocketRegular,
  TabDesktopMultipleRegular,
  BotRegular,
  SettingsRegular,
  PlugConnectedRegular,
} from '@fluentui/react-icons'
import type { ReactNode } from 'react'
import type { Profile } from '../types'
import { SelectBox } from './controls'

export type PageKey =
  | 'topics'
  | 'scan'
  | 'produce'
  | 'groups'
  | 'observe'
  | 'settings'

interface NavItem {
  key: PageKey
  label: string
  icon: ReactNode
}

const NAV: NavItem[] = [
  { key: 'topics', label: 'Topics', icon: <DataUsageRegular /> },
  { key: 'scan', label: '消息查询', icon: <TabDesktopMultipleRegular /> },
  { key: 'produce', label: '数据注入', icon: <RocketRegular /> },
  { key: 'groups', label: 'Consumer Groups', icon: <PlugConnectedRegular /> },
  { key: 'observe', label: 'AI 观测区', icon: <BotRegular /> },
  { key: 'settings', label: '设置', icon: <SettingsRegular /> },
]

const useStyles = makeStyles({
  shell: {
    display: 'grid',
    gridTemplateRows: '48px 1fr',
    gridTemplateColumns: '220px 1fr',
    gridTemplateAreas: '"top top" "side main"',
    height: '100%',
    width: '100%',
  },
  topbar: {
    gridArea: 'top',
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
    padding: '0 12px',
    borderBottom: `1px solid ${tokens.colorNeutralStroke1}`,
    background: tokens.colorNeutralBackground1,
  },
  sidebar: {
    gridArea: 'side',
    borderRight: `1px solid ${tokens.colorNeutralStroke1}`,
    background: tokens.colorNeutralBackground1,
    padding: '8px 4px',
    overflowY: 'auto',
  },
  main: {
    gridArea: 'main',
    overflow: 'auto',
    padding: '16px',
    background: tokens.colorNeutralBackground2,
  },
  navItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '10px',
    padding: '8px 12px',
    borderRadius: '4px',
    cursor: 'pointer',
    color: tokens.colorNeutralForeground2,
    fontSize: '14px',
    userSelect: 'none',
    '&:hover': {
      background: tokens.colorNeutralBackground1Hover,
      color: tokens.colorNeutralForeground1,
    },
  },
  navItemActive: {
    background: tokens.colorBrandBackground2,
    color: tokens.colorBrandForeground1,
    fontWeight: 600,
    '&:hover': {
      background: tokens.colorBrandBackground2Hover,
      color: tokens.colorBrandForeground1,
    },
  },
  brand: {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    paddingLeft: '4px',
  },
})

export interface LayoutProps {
  page: PageKey
  onNavigate: (p: PageKey) => void
  profiles: Profile[]
  activeProfile: string
  onSelectProfile: (name: string) => void
  connected: boolean
  port: number | null
  actor: string
  children: ReactNode
}

export function Layout({
  page,
  onNavigate,
  profiles,
  activeProfile,
  onSelectProfile,
  connected,
  port,
  actor,
  children,
}: LayoutProps) {
  const styles = useStyles()
  return (
    <div className={styles.shell}>
      {/* Top bar */}
      <header className={styles.topbar}>
        <div className={styles.brand}>
          <RocketRegular fontSize={20} style={{ color: tokens.colorBrandForeground1 }} />
          <Subtitle1>Kafka Pilot</Subtitle1>
        </div>

        <Divider vertical style={{ height: 24 }} />

        <Body1 className="kp-muted">Profile</Body1>
        {/* Native select: Fluent Dropdown popup crashed the whole tree on click. */}
        <SelectBox
          ariaLabel="选择 Profile"
          value={activeProfile}
          onChange={onSelectProfile}
          disabled={profiles.length === 0}
          style={{ minWidth: 180 }}
          options={
            profiles.length === 0
              ? [{ value: '', label: '（无 profile，请在设置中添加）', disabled: true }]
              : profiles.map((p) => ({ value: p.name, label: p.name }))
          }
        />

        <div className="kp-grow" />

        <Tooltip content={connected ? '后端已连接' : '后端未响应'} relationship="label">
          <span
            className={connected ? 'kp-pill kp-pill--ok' : 'kp-pill kp-pill--error'}
            style={{ minWidth: 56, justifyContent: 'center' }}
          >
            {connected ? '在线' : '离线'}
          </span>
        </Tooltip>
        {port != null && (
          <Body1 className="kp-mono kp-muted">:{port}</Body1>
        )}
        <Toolbar size="small">
          <ToolbarButton appearance="subtle">{actor}</ToolbarButton>
        </Toolbar>
      </header>

      {/* Sidebar */}
      <nav className={styles.sidebar}>
        {NAV.map((item) => (
          <div
            key={item.key}
            className={
              page === item.key
                ? `${styles.navItem} ${styles.navItemActive}`
                : styles.navItem
            }
            onClick={() => onNavigate(item.key)}
          >
            {item.icon}
            {item.label}
          </div>
        ))}
      </nav>

      {/* Main content */}
      <main className={styles.main}>{children}</main>
    </div>
  )
}
