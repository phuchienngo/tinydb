package src.sstable

import java.io.Closeable
import java.nio.file.Path

class SSTableWriter(
  private val dbPath: Path,
  private val ssTableIndex: Long,
  private val level: Int,
  private val blockSize: Int,
  private val restartInterval: Int
): Closeable {
  override fun close() {

  }
}