package src

import java.io.Closeable
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class DBLock: Closeable {
  private val channel: FileChannel
  private val lock: FileLock?

  constructor(dbPath: Path) {
    val lockPath = dbPath.resolve("LOCK")
    this.channel = FileChannel.open(
      lockPath,
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE,
      StandardOpenOption.READ
    )
    try {
      this.lock = channel.tryLock()
    } catch (e: Exception) {
      closeQuietly(channel)
      throw e
    }

    if (lock == null) {
      throw IllegalStateException("Unable to acquire lock on $dbPath")
    }
  }

  override fun close() {
    try {
      lock?.release()
    } finally {
      closeQuietly(channel)
    }
  }

  private fun closeQuietly(channel: FileChannel) {
    try {
      channel.close()
    } catch (_: Exception) {
      // ignored
    }
  }
}