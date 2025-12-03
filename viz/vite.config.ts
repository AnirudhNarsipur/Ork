import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import fs from 'fs'
import path from 'path'

const EVENT_LOGS_DIR = path.resolve(__dirname, '../event_logs')

function eventLogsPlugin() {
  return {
    name: 'event-logs-api',
    configureServer(server: any) {
      // List event log files
      server.middlewares.use('/api/event-logs', (_req: any, res: any) => {
        try {
          const files = fs.readdirSync(EVENT_LOGS_DIR)
            .filter((f: string) => f.endsWith('.jsonl') || f.endsWith('.json'))
            .sort()
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ files }))
        } catch (err) {
          res.statusCode = 500
          res.end(JSON.stringify({ error: `${err}` }))
        }
      })

      // Serve individual event log files
      server.middlewares.use('/api/event-log/', (req: any, res: any, next: any) => {
        const fileName = decodeURIComponent(req.url?.replace(/^\//, '') || '')
        if (!fileName) {
          return next()
        }

        const filePath = path.join(EVENT_LOGS_DIR, fileName)

        // Security check: ensure we're still in the event_logs directory
        if (!filePath.startsWith(EVENT_LOGS_DIR)) {
          res.statusCode = 403
          res.end(JSON.stringify({ error: 'Access denied' }))
          return
        }

        try {
          const content = fs.readFileSync(filePath, 'utf-8')
          res.setHeader('Content-Type', 'text/plain')
          res.end(content)
        } catch (err) {
          res.statusCode = 404
          res.end(JSON.stringify({ error: `File not found: ${fileName}` }))
        }
      })
    }
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), eventLogsPlugin()],
})
