import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'

import { WebSocket, WebSocketServer } from 'ws'

import { TTSBackend } from '../backend/backend'
import { RealtimeSTTBackend } from '../backend/realtime/realtime'
import { getLogger } from '../logger'

/**
 * Демо-страница «колонка в браузере» (DEMO_PORT, по умолчанию 31117): отладка без станции.
 *  - /realtime — микрофон браузера (PCM16 16 кГц) → тот же RealtimeDialog, что у станции → звук (PCM16 24 кГц)
 *  - /text     — текст → тот же processor (промпт, память, MCP) → ответ текстом и голосом (синтез станции)
 */

interface DemoOptions {
  port: number
  processorUrl: string
  realtime: RealtimeSTTBackend
  tts: TTSBackend
}

const logger = getLogger()
const PAGE = path.join(__dirname, 'demo.html')

function realtimeSession (socket: WebSocket, realtime: RealtimeSTTBackend): void {
  let dialog: ReturnType<RealtimeSTTBackend['createDialog']> | null = null
  const sendJson = (data: object): void => {
    if (socket.readyState === socket.OPEN) socket.send(JSON.stringify(data))
  }

  socket.on('message', async (data, isBinary) => {
    if (isBinary) {
      await dialog?.appendPcm16k(Buffer.from(data as Buffer))
      return
    }
    const message = JSON.parse(data.toString()) as { type: string }
    if (message.type === 'start') {
      dialog?.close()
      const current = realtime.createDialog({ continuous: true })
      dialog = current
      current.onEvent = event => sendJson(event)
      sendJson({ type: 'listening' })
      void (async () => {
        for await (const pcm of current.audio()) {
          if (socket.readyState === socket.OPEN) socket.send(pcm, { binary: true })
        }
        sendJson({ type: 'done' })
        current.close()
      })()
    } else if (message.type === 'stop') {
      dialog?.close()
      dialog = null
    }
  })
  socket.on('close', () => dialog?.close())
}

async function askProcessor (processorUrl: string, text: string): Promise<{ ms: number, parts: string[] }> {
  const started = Date.now()
  const parts: string[] = []
  await new Promise<void>((resolve, reject) => {
    const ws = new WebSocket(processorUrl.replace(/^http/, 'ws'))
    ws.on('open', () => {
      ws.send(JSON.stringify({ data: { directiveFunctions: {} }, type: 'prepare' }))
      ws.send(JSON.stringify({
        data: { metadata: { internal: { info: { duid: 'demo-browser' } }, readyToUse: {} }, text },
        type: 'process'
      }))
    })
    ws.on('message', raw => {
      const message = JSON.parse(raw.toString()) as { data?: { finished?: boolean, text?: string } }
      if (message.data?.text) parts.push(message.data.text)
    })
    ws.on('close', () => resolve())
    ws.on('error', reject)
    setTimeout(() => ws.close(), 60_000)
  })
  return { ms: Date.now() - started, parts }
}

function textSession (socket: WebSocket, options: DemoOptions): void {
  socket.on('message', async data => {
    const { text } = JSON.parse(data.toString()) as { text: string }
    try {
      const { ms, parts } = await askProcessor(options.processorUrl, text)
      socket.send(JSON.stringify({ ms, parts, type: 'answer' }))
      const spoken = parts.at(-1)
      if (spoken) {
        const ttsStarted = Date.now()
        const audio = await options.tts.synthesize({ text: spoken })
        socket.send(JSON.stringify({ ms: Date.now() - ttsStarted, type: 'tts' }))
        socket.send(audio.voiceOutput, { binary: true })
      }
    } catch (error) {
      socket.send(JSON.stringify({ error: String(error), type: 'error' }))
    }
  })
}

export function startDemoServer (options: DemoOptions): void {
  const server = http.createServer((request, response) => {
    if (request.url === '/' || request.url === '/index.html') {
      response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
      response.end(fs.readFileSync(PAGE))
      return
    }
    response.writeHead(404).end()
  })
  const wss = new WebSocketServer({ noServer: true })
  server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, client => {
      if (request.url === '/realtime') {
        realtimeSession(client, options.realtime)
      } else if (request.url === '/text') {
        textSession(client, options)
      } else {
        client.close()
      }
    })
  })
  server.listen(options.port, () => logger.info(`Demo page on :${options.port}`))
}
