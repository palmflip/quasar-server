/**
 * Режет поток Ogg на целые страницы: декодер станции получает каждую страницу целиком,
 * хотя HTTP отдаёт байты кусками произвольной длины.
 *
 * Страница Ogg: "OggS", версия, флаги, granule (8), serial (4), seq (4), crc (4),
 * число сегментов N, таблица длин сегментов (N байт), затем данные суммарной длины.
 */
export async function * oggPages (chunks: AsyncIterable<Uint8Array>): AsyncGenerator<Buffer> {
  let buffer = Buffer.alloc(0)
  for await (const chunk of chunks) {
    buffer = Buffer.concat([buffer, chunk])
    while (true) {
      const start = buffer.indexOf('OggS')
      if (start === -1) {
        break
      }
      if (start > 0) {
        buffer = buffer.subarray(start)
      }
      if (buffer.length < 27) {
        break
      }
      const segments = buffer[26]!
      const headerLength = 27 + segments
      if (buffer.length < headerLength) {
        break
      }
      let bodyLength = 0
      for (let i = 0; i < segments; i++) {
        bodyLength += buffer[27 + i]!
      }
      const pageLength = headerLength + bodyLength
      if (buffer.length < pageLength) {
        break
      }
      yield Buffer.from(buffer.subarray(0, pageLength))
      buffer = buffer.subarray(pageLength)
    }
  }
  if (buffer.length > 0) {
    yield Buffer.from(buffer)
  }
}
