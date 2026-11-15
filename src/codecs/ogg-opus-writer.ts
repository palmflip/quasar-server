import { OpusEncoder } from '@discordjs/opus'

/**
 * PCM16 моно → Ogg Opus страницами, чтобы отдавать станции потоком (как TtsSpeak audio/opus).
 * Каждая страница — один пакет Opus (20 мс): станция начинает играть с первых страниц.
 */

const CRC_TABLE = (() => {
  const table = new Uint32Array(256)
  for (let i = 0; i < 256; i++) {
    let r = i << 24
    for (let j = 0; j < 8; j++) {
      r = (r & 0x80_00_00_00) ? ((r << 1) ^ 0x04_C1_1D_B7) : (r << 1)
    }
    table[i] = r >>> 0
  }
  return table
})()

function oggCrc (data: Buffer): number {
  let crc = 0
  for (const byte of data) {
    crc = ((crc << 8) ^ CRC_TABLE[((crc >>> 24) ^ byte) & 0xFF]!) >>> 0
  }
  return crc
}

class OggPageWriter {
  private sequence = 0
  private readonly serial = Math.floor(Math.random() * 0xFF_FF_FF_FF)

  page (packet: Buffer, granule: bigint, flags: number): Buffer {
    const segments: number[] = []
    let left = packet.length
    while (left >= 255) {
      segments.push(255)
      left -= 255
    }
    segments.push(left)
    const header = Buffer.alloc(27 + segments.length)
    header.write('OggS', 0, 'ascii')
    header.writeUInt8(0, 4)
    header.writeUInt8(flags, 5)
    header.writeBigInt64LE(granule, 6)
    header.writeUInt32LE(this.serial, 14)
    header.writeUInt32LE(this.sequence++, 18)
    header.writeUInt32LE(0, 22)
    header.writeUInt8(segments.length, 26)
    Buffer.from(segments).copy(header, 27)
    const page = Buffer.concat([header, packet])
    page.writeUInt32LE(oggCrc(page), 22)
    return page
  }
}

/** sampleRate входного PCM: 8000, 12000, 16000, 24000 или 48000 (частоты Opus) */
export async function * pcmToOggOpus (pcm: AsyncIterable<Buffer>, sampleRate: number): AsyncGenerator<Buffer> {
  const encoder = new OpusEncoder(sampleRate, 1)
  const frameSamples = sampleRate / 50 // 20 мс
  const frameBytes = frameSamples * 2
  const granulePerFrame = 960n // granule Opus всегда в отсчётах 48 кГц
  const writer = new OggPageWriter()

  const head = Buffer.alloc(19)
  head.write('OpusHead', 0, 'ascii')
  head.writeUInt8(1, 8) // версия
  head.writeUInt8(1, 9) // каналы
  head.writeUInt16LE(312, 10) // pre-skip
  head.writeUInt32LE(sampleRate, 12)
  head.writeInt16LE(0, 16)
  head.writeUInt8(0, 18)
  yield writer.page(head, 0n, 0x02)

  const vendor = Buffer.from('v3rt3p realtime', 'ascii')
  const tags = Buffer.alloc(8 + 4 + vendor.length + 4)
  tags.write('OpusTags', 0, 'ascii')
  tags.writeUInt32LE(vendor.length, 8)
  vendor.copy(tags, 12)
  tags.writeUInt32LE(0, 12 + vendor.length)
  yield writer.page(tags, 0n, 0)

  let pending = Buffer.alloc(0)
  let granule = 0n
  let last: Buffer | null = null
  for await (const chunk of pcm) {
    pending = Buffer.concat([pending, chunk])
    while (pending.length >= frameBytes) {
      const frame = pending.subarray(0, frameBytes)
      pending = pending.subarray(frameBytes)
      granule += granulePerFrame
      if (last) {
        yield last
      }
      last = writer.page(encoder.encode(frame), granule, 0)
    }
  }
  if (pending.length > 0) {
    const frame = Buffer.alloc(frameBytes)
    pending.copy(frame)
    granule += granulePerFrame
    if (last) {
      yield last
    }
    last = writer.page(encoder.encode(frame), granule, 0)
  }
  if (last) {
    // последняя страница с флагом конца потока
    last.writeUInt8(0x04, 5)
    last.writeUInt32LE(0, 22)
    last.writeUInt32LE(oggCrc(last), 22)
    yield last
  }
}
