package src.config

import java.nio.file.Path

data class Config(
  val dbPath: Path,
  val memTableEntriesLimit: Long,
  val memTableSizeLimit: Long,
  val ssTableBlockSize: Int,
  val ssTableRestartInterval: Int
)