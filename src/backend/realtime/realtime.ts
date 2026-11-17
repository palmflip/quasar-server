import fs from 'node:fs'

import { Span } from '@sentry/node'
import { WebSocket } from 'ws'

import { OpusProcessor } from '../../codecs/opus-processor'
import { getLogger } from '../../logger'
import { STTBackend, STTBackendSession } from '../backend'

/**
 * Режим gpt-realtime (REALTIME_MODE=true): звук станции идёт прямо в OpenAI Realtime API,
 * конец фразы определяет сервер OpenAI, ответ приходит звуком. Распознавание, processor и
 * отдельный синтез не участвуют. Реализовано как STT-бэкенд: VoiceInputHandler получает от него
 * текст фразы и конец высказывания, а ответный звук забирает uniproxy-connection через takePending.
 */

export interface RealtimeSettings {
  apiKey: string
  /** Непрерывный разговор с перебиванием (демо): микрофон открыт и во время ответа, речь человека глушит ответ */
  continuous?: boolean
  instructions: string
  /** Внутренняя ручка processor с токеном MCP (пусто — без инструментов): http://processor:8080/internal/mcp-token/bannie */
  mcpTokenUrl: string
  /** Только эти инструменты MCP (пусто — все): модель не увидит лишних и не будет по ним ходить */
  mcpAllowedTools: string[]
  model: string
  silenceMs: number
  /** semantic — конец фразы по смыслу (пауза после «Яндекс» не обрывает), server — по тишине silenceMs */
  turnDetection: 'semantic' | 'server'
  /** Для semantic: low — дольше ждёт, high — отвечает раньше */
  eagerness: 'auto' | 'high' | 'low' | 'medium'
  transcriptionModel: string
  voice: string
}

class Channel<T> {
  private readonly items: T[] = []
  private waiting: ((value: IteratorResult<T>) => void) | null = null
  private closed = false

  push (item: T): void {
    if (this.closed) return
    if (this.waiting) {
      const resolve = this.waiting
      this.waiting = null
      resolve({ done: false, value: item })
    } else {
      this.items.push(item)
    }
  }

  close (): void {
    this.closed = true
    if (this.waiting) {
      const resolve = this.waiting
      this.waiting = null
      resolve({ done: true, value: undefined })
    }
  }

  async * [Symbol.asyncIterator] (): AsyncGenerator<T> {
    while (true) {
      if (this.items.length > 0) {
        yield this.items.shift()!
        continue
      }
      if (this.closed) return
      const next = await new Promise<IteratorResult<T>>(resolve => {
        this.waiting = resolve
      })
      if (next.done) return
      yield next.value
    }
  }
}

/** PCM16 моно 16 кГц → 24 кГц линейной интерполяцией (Realtime принимает PCM только 24 кГц) */
function upsample16to24 (input: Buffer): Buffer {
  const inSamples = Math.floor(input.length / 2)
  const outSamples = Math.floor(inSamples * 3 / 2)
  const output = Buffer.alloc(outSamples * 2)
  for (let i = 0; i < outSamples; i++) {
    const position = i * 2 / 3
    const index = Math.floor(position)
    const fraction = position - index
    const a = input.readInt16LE(Math.min(index, inSamples - 1) * 2)
    const b = input.readInt16LE(Math.min(index + 1, inSamples - 1) * 2)
    output.writeInt16LE(Math.round(a + (b - a) * fraction), i * 2)
  }
  return output
}

/** Фраза из одного слова вызова: человек сказал «Яндекс» и сделал паузу перед вопросом */
const WAKE_WORD_ONLY = /^\s*((яндекс|алиса)[\s.,!?…-]*)?$/i // пустая фраза (шум, эхо) — тоже не вопрос

export type RealtimeEvent = { type: string } & Record<string, unknown>

export class RealtimeDialog {
  /** Диалог, чей ответ сейчас звучит на станции: новый вопрос его обрывает */
  static active: RealtimeDialog | null = null
  private static pending: RealtimeDialog | null = null
  /** Наблюдатель за ходом диалога (демо-страница) */
  onEvent: ((event: RealtimeEvent) => void) | null = null
  assistantText = ''
  /** Ответ уже идёт: при закрытии распознавания соединение не рвём */
  responding = false
  userText = ''
  private readonly audioChannel = new Channel<Buffer>()
  private endOfUtterance: ((text: string) => void) | null = null
  /** Промежуточный текст для станции: без него через ~5 с она решает, что речи нет */
  private partial: ((text: string) => void) | null = null
  private partialText = ''
  private readonly createdAt = Date.now()
  private eouSent = false
  private firstAudioLogged = false
  /** Звук ответа до проверки расшифровки: если фраза — одно слово вызова, ответ отменяем */
  private held: Buffer[] = []
  private responseDone = false
  /** Ответ закончился вызовом MCP: после его завершения просим продолжение с данными */
  private awaitingTool = false
  private verdict: 'drop' | 'idle' | 'pass' | 'pending' = 'idle'
  private readonly logger = getLogger<RealtimeDialog>()
  private readonly ready: Promise<void>
  private speechStoppedAt = 0
  private readonly ws: WebSocket

  private readonly continuous: boolean
  private tokenTimer: NodeJS.Timeout | null = null
  /** Свежий токен MCP в сессию: при выдаче тёплой сессии станции и раз в 2 минуты */
  refreshTools: () => Promise<void> = async () => {}
  private audioBytes = 0
  /** Ответ создаём сами: только когда OpenAI уже получил список инструментов MCP */
  private toolsReady = true
  private createWhenReady = false
  /** OpenAI держит один активный ответ: новый ставим в очередь до response.done */
  private responseActive = false
  private createQueued = false

  constructor (settings: RealtimeSettings) {
    this.continuous = settings.continuous ?? false
    this.ws = new WebSocket(`wss://api.openai.com/v1/realtime?model=${encodeURIComponent(settings.model)}`, {
      headers: { Authorization: `Bearer ${settings.apiKey}` }
    })
    this.ready = new Promise((resolve, reject) => {
      this.ws.once('open', async () => {
        const tools = await this.mcpTools(settings)
        this.toolsReady = tools.length === 0
        if (tools.length > 0) {
          // access-токен живёт 5 минут: в долгом разговоре обновляем инструменты с новым токеном
          this.refreshTools = async () => {
            const fresh = await this.mcpTools(settings)
            if (fresh.length > 0 && this.ws.readyState === this.ws.OPEN) {
              this.send({ session: { tools: fresh, type: 'realtime' }, type: 'session.update' })
            }
          }
          this.tokenTimer = setInterval(() => void this.refreshTools(), 2 * 60 * 1000)
        }
        this.send({
          session: {
            audio: {
              input: {
                format: { rate: 24_000, type: 'audio/pcm' },
                transcription: { language: 'ru', model: settings.transcriptionModel },
                turn_detection: settings.turnDetection === 'semantic'
                  ? { create_response: false, eagerness: settings.eagerness, interrupt_response: this.continuous, type: 'semantic_vad' }
                  : { create_response: false, interrupt_response: this.continuous, silence_duration_ms: settings.silenceMs, type: 'server_vad' }
              },
              output: {
                format: { rate: 24_000, type: 'audio/pcm' },
                voice: settings.voice
              }
            },
            instructions: settings.instructions,
            output_modalities: ['audio'],
            tools,
            type: 'realtime'
          },
          type: 'session.update'
        })
        resolve()
      })
      this.ws.once('error', reject)
    })
    this.ws.on('message', data => this.handle(JSON.parse(data.toString()) as Record<string, unknown>))
    this.ws.on('close', () => this.audioChannel.close())
  }

  /** MCP-сервер (bannie) прямо в сессии: инструменты вызывает OpenAI, токен берём у processor */
  private async mcpTools (settings: RealtimeSettings): Promise<object[]> {
    if (!settings.mcpTokenUrl) return []
    try {
      const url = new URL(settings.mcpTokenUrl)
      url.searchParams.set('min_valid', '150')
      const response = await fetch(url)
      const { access_token: token, url } = await response.json() as { access_token: string | null, url: string }
      if (!token) return []
      const label = url.pathname.split('/').pop() ?? 'mcp'
      return [{
        authorization: token,
        require_approval: 'never',
        server_label: label,
        server_url: url,
        type: 'mcp',
        ...(settings.mcpAllowedTools.length > 0 ? { allowed_tools: settings.mcpAllowedTools } : {})
      }]
    } catch (error) {
      this.logger.warn(`Realtime: no MCP token, tools disabled: ${error}`)
      return []
    }
  }

  /** Диалог, у которого закончилась фраза и идёт ответ: его забирает uniproxy-connection */
  static takePending (): RealtimeDialog | null {
    const dialog = RealtimeDialog.pending
    RealtimeDialog.pending = null
    return dialog
  }

  async appendPcm16k (pcm: Buffer): Promise<void> {
    await this.ready
    if (this.ws.readyState !== this.ws.OPEN) return
    this.audioBytes += pcm.length
    this.send({ audio: upsample16to24(pcm).toString('base64'), type: 'input_audio_buffer.append' })
  }

  /** PCM16 24 кГц ответа по мере генерации */
  audio (): AsyncIterable<Buffer> {
    return this.audioChannel
  }

  close (): void {
    if (this.tokenTimer) clearInterval(this.tokenTimer)
    if (RealtimeDialog.active === this) RealtimeDialog.active = null
    this.audioChannel.close()
    if (this.ws.readyState === this.ws.OPEN || this.ws.readyState === this.ws.CONNECTING) {
      this.ws.close()
    }
  }

  onEndOfUtterance (callback: (text: string) => void): void {
    this.endOfUtterance = callback
  }

  onPartial (callback: (text: string) => void): void {
    this.partial = callback
  }

  /** Сессия открыта и инструменты загружены — можно отдавать станции */
  get warm (): boolean {
    return this.ws.readyState === this.ws.OPEN && this.toolsReady
  }

  get ageMs (): number {
    return Date.now() - this.createdAt
  }

  private requestResponse (): void {
    if (this.responseActive) {
      this.createQueued = true
      return
    }
    this.responseActive = true
    this.send({ type: 'response.create' })
  }

  /** Ответ на фразу: сразу, если инструменты готовы, иначе — когда OpenAI их получит */
  private createResponse (): void {
    if (this.toolsReady) {
      this.requestResponse()
    } else {
      this.logger.info('Realtime: waiting for MCP tools before answering')
      this.createWhenReady = true
    }
  }

  /** Фраза настоящая: отдаём придержанный звук, станции — конец высказывания */
  private accept (): void {
    this.verdict = 'pass'
    this.responding = true
    RealtimeDialog.pending = this
    for (const audio of this.held) this.audioChannel.push(audio)
    this.held = []
    this.emitEndOfUtterance()
    if (this.responseDone) {
      this.logger.info(`Realtime assistant: ${this.assistantText} (${Date.now() - this.speechStoppedAt} ms)`)
      this.onEvent?.({ ms: Date.now() - this.speechStoppedAt, text: this.assistantText, type: 'assistant' })
      this.audioChannel.close()
    }
  }

  private emitEndOfUtterance (): void {
    if (this.eouSent) return
    this.eouSent = true
    this.endOfUtterance?.(this.userText)
  }

  private handle (event: Record<string, unknown>): void {
    if (process.env['REALTIME_DEBUG'] && !String(event['type']).includes('delta')) {
      this.logger.info(`RT event ${String(event['type'])}${event['type'] === 'response.done' ? ' ' + JSON.stringify((event['response'] as { output?: { type: string }[] })?.output?.map(item => item.type)) : ''}`)
    }
    switch (event['type']) {
      case 'input_audio_buffer.speech_started': {
        this.logger.info(`Realtime speech started (${this.audioBytes} bytes of audio so far)`)
        // Станция сдаётся через ~5 с без промежуточного результата: сообщаем, что речь слышна
        if (!this.continuous && !this.partialText) this.partial?.('…')
        if (this.continuous) {
          // Человек заговорил: сервер сам отменяет ответ (interrupt_response), клиент глушит звук
          this.onEvent?.({ type: 'speech-started' })
          if (this.responseActive) this.send({ type: 'response.cancel' })
          this.awaitingTool = false
          this.createQueued = false
          this.verdict = 'idle'
          this.held = []
        }
        break
      }
      case 'input_audio_buffer.speech_stopped': {
        if (this.verdict === 'pass' && !this.continuous) break
        this.speechStoppedAt = Date.now()
        this.verdict = 'pending'
        this.held = []
        this.responseDone = false
        this.assistantText = ''
        this.firstAudioLogged = false
        this.createResponse()
        // Без расшифровки не знаем, не одно ли это «Яндекс»; не пришла за 2 с — отвечаем как есть
        setTimeout(() => {
          if (this.verdict === 'pending') this.accept()
        }, 2000)
        break
      }
      case 'conversation.item.input_audio_transcription.delta': {
        this.partialText += String(event['delta'] ?? '')
        if (this.partialText.trim()) this.partial?.(this.partialText.trim())
        break
      }
      case 'conversation.item.input_audio_transcription.completed': {
        const text = String(event['transcript'] ?? '').trim()
        this.partialText = ''
        // Станция ждёт, пока её «слышат»: даже одно «Яндекс» показываем ей как промежуточный текст
        if (text) this.partial?.(text)
        this.logger.info(`Realtime user: ${text}`)
        this.onEvent?.({ text, type: 'user' })
        if (this.verdict !== 'pending') break
        if (WAKE_WORD_ONLY.test(text)) {
          // Только «Яндекс» — отменяем ответ и слушаем дальше, вопрос ещё впереди
          this.logger.info('Realtime: only the wake word, waiting for the question')
          this.onEvent?.({ type: 'wake-only' })
          this.verdict = 'drop'
          this.held = []
          this.createWhenReady = false
          this.send({ type: 'response.cancel' })
          break
        }
        this.userText = text
        this.accept()
        break
      }
      case 'response.output_audio.delta': {
        if (this.verdict === 'drop' || this.verdict === 'idle') break
        if (!this.firstAudioLogged) {
          this.firstAudioLogged = true
          this.logger.info(`Realtime first audio ${Date.now() - this.speechStoppedAt} ms after end of speech`)
          this.onEvent?.({ ms: Date.now() - this.speechStoppedAt, type: 'first-audio' })
        }
        const audio = Buffer.from(String(event['delta']), 'base64')
        if (this.verdict === 'pending') {
          this.held.push(audio)
        } else {
          this.audioChannel.push(audio)
        }
        break
      }
      case 'response.output_audio_transcript.delta': {
        if (this.verdict === 'drop') break
        this.assistantText += String(event['delta'] ?? '')
        break
      }
      case 'response.created': {
        this.responseActive = true
        break
      }
      case 'response.done': {
        this.responseActive = false
        if (this.createQueued) {
          this.createQueued = false
          this.requestResponse()
        }
        const output = (event['response'] as { output?: { type: string }[] } | undefined)?.output ?? []
        if (this.verdict !== 'drop' && output.some(item => item.type === 'mcp_call')) {
          // «Сейчас гляну» + вызов bannie: поток не закрываем, ответ с данными будет следующим
          this.awaitingTool = true
          if (this.verdict === 'pending') this.responseDone = false
          break
        }
        if (this.verdict === 'drop') {
          this.verdict = 'idle'
          break
        }
        if (this.verdict === 'pending') {
          // Ответ готов раньше расшифровки: ждём вердикт, звук придержан
          this.responseDone = true
          break
        }
        this.logger.info(`Realtime assistant: ${this.assistantText} (${Date.now() - this.speechStoppedAt} ms)`)
        this.onEvent?.({ ms: Date.now() - this.speechStoppedAt, text: this.assistantText, type: 'assistant' })
        if (this.continuous) {
          this.verdict = 'idle'
          break
        }
        this.audioChannel.close()
        break
      }
      case 'response.output_item.done': {
        const item = event['item'] as { arguments?: string, error?: unknown, name?: string, output?: string, type?: string } | undefined
        if (item?.type === 'mcp_call') {
          const detail = item.error ? `ERROR ${JSON.stringify(item.error)}` : `ok, ${String(item.output ?? '').length} chars`
          this.logger.info(`Realtime MCP ${item.name}(${item.arguments}) → ${detail}`)
          this.onEvent?.({ arguments: item.arguments, error: item.error, name: item.name, type: 'mcp-detail' })
        }
        break
      }
      case 'mcp_list_tools.completed': {
        this.logger.info('Realtime MCP tools listed')
        this.toolsReady = true
        if (this.createWhenReady) {
          this.createWhenReady = false
          this.requestResponse()
        }
        break
      }
      case 'response.mcp_call.completed':
      case 'response.mcp_call.failed': {
        this.logger.info(`Realtime ${String(event['type'])} after ${Date.now() - this.speechStoppedAt} ms`)
        this.onEvent?.({ ms: Date.now() - this.speechStoppedAt, name: (event['item'] as { name?: string } | undefined)?.name, type: String(event['type']) })
        if (this.awaitingTool) {
          this.awaitingTool = false
          this.requestResponse()
        }
        break
      }
      case 'error': {
        if ((event['error'] as { code?: string } | undefined)?.code === 'conversation_already_has_active_response') {
          this.createQueued = true
          break
        }
        if ((event['error'] as { code?: string } | undefined)?.code === 'response_cancel_not_active') break
        this.logger.error(`Realtime error: ${JSON.stringify(event['error'])}`)
        this.onEvent?.({ error: event['error'], type: 'error' })
        this.emitEndOfUtterance()
        this.audioChannel.close()
        break
      }
      default:
    }
  }

  private send (event: object): void {
    this.ws.send(JSON.stringify(event))
  }
}

class RealtimeSTTSession extends STTBackendSession {
  private readonly opus: OpusProcessor

  constructor (private readonly dialog: RealtimeDialog) {
    super()
    this.opus = new OpusProcessor(pcm => dialog.appendPcm16k(pcm), async () => {})
    dialog.onEndOfUtterance(text => this.chunkTranscribed({ endOfUtt: true, text }))
    dialog.onPartial(text => this.chunkTranscribed({ endOfUtt: false, text }))
  }

  close (): void {
    // После конца фразы соединение нужно для ответа: его закроет uniproxy-connection
    if (!this.dialog.responding) {
      this.dialog.close()
    }
  }

  async transcribeChunk (chunk: Buffer): Promise<void> {
    await this.opus.handleAudioData(chunk)
  }
}

export type RealtimeBackendSettings = Omit<RealtimeSettings, 'instructions'> & { instructionsPath: string }

export class RealtimeSTTBackend implements STTBackend {
  /** Заранее открытая сессия с загруженными инструментами: на «Яндекс» не ждём MCP */
  private warmDialog: RealtimeDialog | null = null
  private readonly logger = getLogger<RealtimeSTTBackend>()

  constructor (private readonly settings: RealtimeBackendSettings) {}

  /** Держать одну тёплую сессию; пересоздавать раз в 10 минут, чтобы не протухла */
  startWarmPool (): void {
    this.prewarm()
    setInterval(() => {
      if (this.warmDialog && this.warmDialog.ageMs > 10 * 60 * 1000) {
        this.warmDialog.close()
        this.warmDialog = null
        this.prewarm()
      }
    }, 60 * 1000)
  }

  private prewarm (): void {
    if (this.warmDialog) return
    try {
      this.warmDialog = this.createDialog()
    } catch (error) {
      this.logger.warn(`Realtime prewarm failed: ${error}`)
    }
  }

  private takeWarm (): RealtimeDialog {
    const dialog = this.warmDialog
    this.warmDialog = null
    setTimeout(() => this.prewarm(), 0)
    if (dialog && dialog.warm) {
      this.logger.info('Realtime: using prewarmed session')
      void dialog.refreshTools()
      return dialog
    }
    dialog?.close()
    return this.createDialog()
  }

  /** Новый диалог; инструкции перечитываются каждый раз — правки файла применяются без перезапуска */
  createDialog (overrides: Partial<RealtimeSettings> = {}): RealtimeDialog {
    const { instructionsPath, ...rest } = this.settings
    return new RealtimeDialog({ ...rest, instructions: fs.readFileSync(instructionsPath, 'utf8'), ...overrides })
  }

  async startTranscribing (_span?: Span): Promise<STTBackendSession> {
    // Новый вопрос (в том числе «Яндекс» посреди ответа) обрывает звучащий ответ
    RealtimeDialog.active?.close()
    const dialog = this.warmDialog !== undefined ? this.takeWarm() : this.createDialog()
    RealtimeDialog.active = dialog
    return new RealtimeSTTSession(dialog)
  }
}
