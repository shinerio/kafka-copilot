import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
// Dev proxy: /api -> Go HTTP server (kp serve --port 18080).
// Build output is embedded into the Go binary via embed.FS
// (see internal/serve), so outDir points there.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:18080',
        changeOrigin: true,
        // SSE: don't buffer the audit stream
        ws: false,
      },
    },
  },
  build: {
    outDir: '../internal/serve/dist',
    emptyOutDir: true,
    sourcemap: false,
    chunkSizeWarningLimit: 1500,
  },
})
