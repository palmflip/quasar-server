import { Span, startInactiveSpan } from '@sentry/node'
import { OpenAI } from 'openai'

import { TTSBackend, TTSRequest, TTSResponse } from '../backend'
import { oggPages } from './ogg-pages'

interface OpenAITSBackendParameters {
  model: string;
  speed: number;
  streamFormat?: 'opus' | 'pcm';
  voice: string;
}

export class OpenAITTSBackend implements TTSBackend {
  constructor (private readonly openAI: OpenAI, private readonly parameters: OpenAITSBackendParameters) {}

  get streamMime (): string {
    return this.parameters.streamFormat === 'pcm' ? 'audio/x-pcm;bit=16;rate=24000' : 'audio/opus'
  }

  async synthesize (request: TTSRequest): Promise<TTSResponse> {
    let span: Span | undefined
    if (request.parentSpan) {
      span = startInactiveSpan({
        name: 'OpenAI TTS synthesizing',
        op: 'openai-tts',
        parentSpan: request.parentSpan
      })
    }
    try {
      const result = await this.openAI.audio.speech.create({
        input: request.text,
        model: this.parameters.model,
        response_format: 'opus',
        speed: this.parameters.speed,
        voice: this.parameters.voice,
      })
      return {
        format: 'audio/opus',
        voiceOutput: Buffer.from(await result.arrayBuffer()),
      }
    } finally {
      span?.end()
    }
  }

  /** OpenAI отдаёт Ogg Opus чанками по мере синтеза — пересылаем станции, не дожидаясь конца */
  async * synthesizeStream (request: TTSRequest): AsyncGenerator<Buffer> {
    const pcm = this.parameters.streamFormat === 'pcm'
    const result = await this.openAI.audio.speech.create({
      input: request.text,
      model: this.parameters.model,
      response_format: pcm ? 'pcm' : 'opus',
      speed: this.parameters.speed,
      voice: this.parameters.voice,
    })
    if (pcm && result.body) {
      // Сырые сэмплы режем по 100 мс (4800 байт): чётное число байт, без половинок сэмплов
      let pending = Buffer.alloc(0)
      for await (const chunk of result.body as unknown as AsyncIterable<Uint8Array>) {
        pending = Buffer.concat([pending, chunk])
        while (pending.length >= 4800) {
          yield Buffer.from(pending.subarray(0, 4800))
          pending = pending.subarray(4800)
        }
      }
      if (pending.length > 0) {
        yield Buffer.from(pending.subarray(0, pending.length - (pending.length % 2)))
      }
      return
    }
    if (!result.body) {
      yield Buffer.from(await result.arrayBuffer())
      return
    }
    yield * oggPages(result.body as unknown as AsyncIterable<Uint8Array>)
  }
}
