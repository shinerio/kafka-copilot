// ErrorBoundary: a render error in any page or widget must not blank the whole
// application. Show the error inline with a retry / reload action instead.

import { Component } from 'react'
import type { ErrorInfo, ReactNode } from 'react'

interface Props {
  children: ReactNode
  label?: string
}

interface State {
  error: Error | null
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null }

  static getDerivedStateFromError(error: Error): State {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('[kafka-pilot] UI render error:', error, info.componentStack)
  }

  render() {
    if (this.state.error) {
      return (
        <div className="kp-error-boundary">
          <h3>界面渲染出错{this.props.label ? `（${this.props.label}）` : ''}</h3>
          <pre>{this.state.error.message}</pre>
          <div className="kp-row">
            <button className="kp-btn" onClick={() => this.setState({ error: null })}>
              重试
            </button>
            <button className="kp-btn" onClick={() => window.location.reload()}>
              重新加载
            </button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}
