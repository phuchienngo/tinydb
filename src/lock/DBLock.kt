package src.lock

import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.channels.FileLock
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists

class DBLock(dbPath: Path) : Closeable {
  private val randomAccessFile: RandomAccessFile
  private val lock: FileLock

  init {
    val lockPath = dbPath.resolve("LOCK")
    if (!lockPath.exists()) {
      Files.createFile(lockPath)
    }
    randomAccessFile = RandomAccessFile(lockPath.toFile(), "rws")
    try {
      lock = randomAccessFile.channel.tryLock()
        ?: throw IllegalStateException("Unable to acquire lock on $dbPath")
    } catch (e: Exception) {
      closeQuietly()
      throw e
    }
  }

  override fun close() {
    try {
      lock.release()
    } catch (_: Exception) {
      // ignored
    } finally {
      closeQuietly()
    }
  }

  private fun closeQuietly() {
    try {
      randomAccessFile.channel.close()
    } catch (_: Exception) {
      // ignored
    }
  }
}