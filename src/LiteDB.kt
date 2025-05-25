package src

import com.google.common.base.Preconditions
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Path

class LiteDB: Closeable {
  private val path: Path
  private val dbLock: DBLock

  constructor(dbPath: Path) {
    path = dbPath
    dbLock = DBLock(path.resolve("LOCK").toFile())
    initialize()
  }

  fun get(key: ByteArray): ByteArray? {
    TODO()
  }

  fun put(key: ByteArray, value: ByteArray) {

  }

  fun delete(key: ByteArray) {

  }

  override fun close() {
    dbLock.close()
  }

  private fun initialize() {
    val current = path.resolve("CURRENT").toFile()
    if (!current.isFile || current.exists()) {
      return
    }
    val lines = current.readLines()
    Preconditions.checkArgument(lines.size == 1, "Invalid CURRENT file")
    val manifestFileName = lines[0]
    Preconditions.checkArgument(
      manifestFileName.matches(Regex.fromLiteral("[0-9]+")),
      "Invalid manifest file name"
    )
  }

}