import type {
  MusicServiceGetAlbumInfoResponse,
  MusicServiceGetAlbumTracksResponse,
  MusicServiceGetArtistAlbumsResponse,
  MusicServiceGetArtistInfoResponse,
  MusicServiceGetArtistTracksResponse,
  MusicServiceGetPlaylistInfoResponse,
  MusicServiceGetPlaylistTracksResponse,
  MusicServiceGetStockPlaylistsResponse,
  MusicServiceGetStockWavesResponse,
  MusicServiceSearchRequest,
  MusicServiceSearchResponse,
  MusicTrackWithUrl
} from '@v3rt3p/types/music'

import { Span } from '@sentry/node'
import { ProcessorDirectiveFunctions, ProcessorPartialResponseWebSocketMessage, ProcessorProcessWebSocketMessage } from '@v3rt3p/types/processor'
import { EventEmitter } from 'node:stream'

export interface AudioMetadataBackend {
  startCapturing(parentSpan?: Span): Promise<AudioMetadataBackendSession>;
}

export interface ProcessorBackend {
  openSession(parentSpan?: Span): Promise<ProcessorSession>;
}

export type ProcessorPartialResponse = ProcessorPartialResponseWebSocketMessage['data']

export type ProcessorRequest = ProcessorProcessWebSocketMessage['data']

export interface ProcessorSession extends EventEmitter<ProcessorSessionEvents> {
  close(): void;
  prepare(directiveFunctions: ProcessorDirectiveFunctions): Promise<void>;
  process(request: ProcessorRequest): Promise<void>;
}

export interface ProcessorSessionEvents {
  close: []
  partialResponse: [ProcessorPartialResponse]
}

export interface STTBackend {
  startTranscribing(span?: Span): Promise<STTBackendSession>;
}

export type STTChunkTranscribeCallback = (result: STTChunkTranscribeResult) => void

export interface STTChunkTranscribeResult {
  endOfUtt: boolean;
  text: string;
}

export interface TTSBackend {
  synthesize(request: TTSRequest): Promise<TTSResponse>;
  /** Звук по мере синтеза (целые страницы Ogg Opus). Нет метода — станция ждёт весь ответ */
  synthesizeStream?(request: TTSRequest): AsyncIterable<Buffer>;
  /** MIME звука из synthesizeStream для TtsSpeak.Format */
  readonly streamMime?: string;
}

export interface TTSRequest {
  parentSpan?: Span
  text: string;
}

export interface TTSResponse {
  format: string;
  voiceOutput: Buffer;
}

export abstract class AudioMetadataBackendSession {
  abstract close (): void
  abstract finish (): Promise<object>
  abstract processChunk (chunk: Buffer): Promise<void>
}

export abstract class MusicBackend {
  abstract advanceQueueBackward (deviceId: string): Promise<void>
  abstract advanceQueueForward (deviceId: string): Promise<void>
  abstract createQueue (queueId: string, itemId: string,
    startFromItemId?: string | undefined, doShuffle?: boolean | undefined): Promise<void>
  abstract getAlbumInfo (
    albumId: string
  ): Promise<MusicServiceGetAlbumInfoResponse>

  abstract getAlbumTracks (
    albumId: string,
    page?: number,
    perPage?: number
  ): Promise<MusicServiceGetAlbumTracksResponse>
  abstract getArtistAlbums (
    artistId: string,
    page?: number,
    perPage?: number
  ): Promise<MusicServiceGetArtistAlbumsResponse>

  abstract getArtistInfo (
    artistId: string
  ): Promise<MusicServiceGetArtistInfoResponse>
  abstract getArtistTracks (
    artistId: string,
    page?: number,
    perPage?: number
  ): Promise<MusicServiceGetArtistTracksResponse>

  abstract getCurrentTrack (deviceId: string): Promise<MusicTrackWithUrl | null>

  abstract getNextTrack (deviceId: string): Promise<MusicTrackWithUrl | null>

  abstract getPlaylistInfo (
    playlistId: string
  ): Promise<MusicServiceGetPlaylistInfoResponse>

  abstract getPlaylistTracks (
    playlistId: string,
    page?: number,
    perPage?: number
  ): Promise<MusicServiceGetPlaylistTracksResponse>
  abstract getStockPlaylists (): Promise<MusicServiceGetStockPlaylistsResponse>
  abstract getStockPlaylists (): Promise<MusicServiceGetStockPlaylistsResponse>
  abstract getStockWaves (): Promise<MusicServiceGetStockWavesResponse>
  abstract getStockWaves (): Promise<MusicServiceGetStockWavesResponse>

  abstract searchMusic (
    request: MusicServiceSearchRequest
  ): Promise<MusicServiceSearchResponse>
}

export abstract class STTBackendSession {
  private callback: null | STTChunkTranscribeCallback = null

  abstract close (): void

  setCallback (callback: STTChunkTranscribeCallback): void {
    this.callback = callback
  }

  abstract transcribeChunk (chunk: Buffer): void

  protected chunkTranscribed (result: STTChunkTranscribeResult): void {
    if (this.callback) {
      this.callback(result)
    }
  }
}
