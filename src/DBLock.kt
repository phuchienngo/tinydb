package src

import java.io.Closeable
import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.channels.FileLock

class DBLock: Closeable {
  private val lockFile: File
  private val channel: FileChannel
  private val lock: FileLock?

  constructor(lockFile: File) {
    this.lockFile = lockFile
    this.channel = RandomAccessFile(lockFile, "rw").channel
    try {
      this.lock = channel.tryLock()
    } catch (e: Exception) {
      closeQuietly(channel)
      throw e
    }

    if (lock == null) {
      throw IllegalStateException("Unable to acquire lock on $lockFile")
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