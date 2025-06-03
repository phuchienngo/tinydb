package src.sstable

import src.proto.manifest.AddFile
import src.proto.manifest.BatchOperation
import src.proto.manifest.ManifestRecord
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import java.nio.file.Path

class SSTableWriterHelper(
  private val dbPath: Path,
  private val targetLevel: Long,
  private val blockSize: Int,
  private val restartInterval: Int,
  currentAvailableSSTableIndex: Long,
) {
  private var currentSSTableIndex = currentAvailableSSTableIndex
  private val changeLogs = BatchOperation.newBuilder()
  private var ssTableWriter = setupNewTable(currentSSTableIndex)

  fun write(key: MemTableKey, value: MemTableValue) {
    val entry = MemTableEntry.newBuilder()
      .setKey(key)
      .setValue(value)
      .build()
    val needNewFile = shouldStartNewSSTable(
      ssTableWriter.dataSize(),
      targetLevel,
      entry.serializedSize,
    )

    if (needNewFile) {
      finishCurrentTable()
      ssTableWriter = setupNewTable(++currentSSTableIndex)
    }

    ssTableWriter.add(entry)
  }

  fun finish(): BatchOperation {
    finishCurrentTable()
    return changeLogs.build()
  }

  private fun shouldStartNewSSTable(currentDataSize: Long, tableLevel: Long, nextEntrySize: Int): Boolean {
    val targetSize = when (tableLevel) {
      0L, 1L -> 2 * 1024 * 1024
      2L -> 20L * 1024 * 1024
      3L -> 200L * 1024 * 1024
      4L -> 2000L * 1024 * 1024
      else -> 200000L * 1024 * 1024
    }
    val estimatedSize = (currentDataSize * 1.2).toLong()
    val recordSize = nextEntrySize + Int.SIZE_BYTES
    return currentDataSize > 0 && estimatedSize + recordSize > targetSize
  }

  private fun finishCurrentTable() {
    ssTableWriter.finish()
    ssTableWriter.close()
  }

  private fun setupNewTable(tableIndex: Long): SSTableWriter {
    changeLogs.addRecords(ManifestRecord.newBuilder()
      .setCurrentSsTable(tableIndex + 1)
    ).addRecords(ManifestRecord.newBuilder()
      .setAddFile(AddFile.newBuilder()
        .addSsTableIndex(tableIndex)
      ).build()
    )
    return SSTableWriter(
      dbPath,
      tableIndex,
      targetLevel,
      blockSize,
      restartInterval,
    )
  }
}