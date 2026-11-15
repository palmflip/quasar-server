import { Span, startInactiveSpan } from '@sentry/node'
import { OpenAI } from 'openai'

import { TTSBackend, TTSRequest, TTSResponse } from '../backend'
import { oggPages } from './ogg-pages'

interface OpenAITSBackendParameters {
  model: string;
  speed: number;
  voice: string;
}

export class OpenAITTSBackend implements TTSBackend {
  constructor (private readonly openAI: OpenAI, private readonly parameters: OpenAITSBackendParameters) {}

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
    const result = await this.openAI.audio.speech.create({
      input: request.text,
      model: this.parameters.model,
      response_format: 'opus',
      speed: this.parameters.speed,
      voice: this.parameters.voice,
    })
    if (!result.body) {
      yield Buffer.from(await result.arrayBuffer())
      return
    }
    yield * oggPages(result.body as unknown as AsyncIterable<Uint8Array>)
  }
}
