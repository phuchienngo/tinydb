package src

import com.google.protobuf.ByteString
import src.config.Config
import src.manifest.Manifest
import src.memtable.MemTable
import src.proto.manifest.AddFile
import src.proto.manifest.BatchOperation
import src.proto.manifest.ManifestRecord
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import src.sstable.SSTableReader
import src.sstable.SSTableWriter
import src.wal.LogReader
import src.wal.LogWriter
import java.io.Closeable
import java.nio.file.Path

class TinyDB: Closeable {
  private val config: Config
  private val dbLock: DBLock
  private val manifest: Manifest
  private var walLogger: LogWriter
  private val memTable: MemTable
  private val openingSSTables: MutableMap<Long, SSTableReader>
  private var sequenceNumber: Long

  constructor(config: Config) {
    this.config = config
    val dbPath = config.dbPath
    dbLock = DBLock(dbPath)
    manifest = Manifest(dbPath)
    memTable = MemTable()
    val logReader = LogReader(dbPath, manifest)
    sequenceNumber = logReader.recover(memTable)
    logReader.close()
    walLogger = LogWriter(dbPath, manifest)
    openingSSTables = mutableMapOf()
    openSSTables(dbPath)
  }

  private fun openSSTables(dbPath: Path) {
    val listSSTableIndexes = manifest.committedSSTableIndexes()
    val openingSSTableIndexes = openingSSTables.keys
    for (openingIndex in openingSSTableIndexes) {
      if (listSSTableIndexes.contains(openingIndex)) {
        continue // Already opened
      }
      openingSSTables.remove(openingIndex)?.close() // Remove stale SSTable
    }
    for (fileIndex in listSSTableIndexes) {
      val ssTableReader = SSTableReader(dbPath, fileIndex)
      openingSSTables.put(fileIndex, ssTableReader)
    }
  }

  fun get(key: ByteArray): ByteArray? {
    val memTableKey = MemTableKey.newBuilder()
      .setKey(ByteString.copyFrom(key))
      .build()

    var memTableValue = memTable.get(memTableKey)
    if (memTableValue != null) {
      return when (memTableValue.dataCase) {
        MemTableValue.DataCase.VALUE -> memTableValue.value.toByteArray()
        else -> null
      }
    }

    for (openingSSTable in openingSSTables.values) {
      val entry = openingSSTable.get(memTableKey) ?: continue
      if (memTableValue == null || entry.value.sequence > memTableValue.sequence) {
        memTableValue = entry.value
      }
    }

    return memTableValue?.let {
      when (it.dataCase) {
        MemTableValue.DataCase.VALUE -> it.value.toByteArray()
        else -> null
      }
    }
  }

  fun put(key: ByteArray, value: ByteArray) {
    val memTableKey = MemTableKey.newBuilder()
      .setKey(ByteString.copyFrom(key))
      .build()
    val memTableValue = MemTableValue.newBuilder()
      .setValue(ByteString.copyFrom(value))
      .setSequence(manifest.committedWalIndex() + 1)
      .build()
    return internalPut(memTableKey, memTableValue)
  }

  fun delete(key: ByteArray) {
    val memTableKey = MemTableKey.newBuilder()
      .setKey(ByteString.copyFrom(key))
      .build()
    val memTableValue = MemTableValue.newBuilder()
      .setTombstone(true)
      .setSequence(sequenceNumber + 1)
      .build()
    return internalPut(memTableKey, memTableValue)
  }

  override fun close() {
    manifest.close()
    walLogger.close()
    for (ssTableReader in openingSSTables.values) {
      ssTableReader.close()
    }
    dbLock.close()
  }

  private fun internalPut(memTableKey: MemTableKey, memTableValue: MemTableValue) {
    walLogger.put(memTableKey, memTableValue)
    memTable.put(memTableKey, memTableValue)
    sequenceNumber += 1
    if (memTable.getEntriesCount() < config.memTableSizeLimit && memTable.getMemTableSize() < config.memTableSizeLimit) {
      return
    }
    writeMemTableToSSTable()
  }

  private fun writeMemTableToSSTable() {
    val oldWalIndex = manifest.committedWalIndex()
    val nextSSTableIndex = manifest.committedSSTableIndex() + 1
    val nextWalIndex = oldWalIndex + 1
    val ssTableWriter = SSTableWriter(
      config.dbPath,
      nextSSTableIndex,
      0,
      config.ssTableBlockSize,
      config.ssTableRestartInterval
    )
    persistMemTableEntry(ssTableWriter)
    ssTableWriter.close()
    val batchOperation = BatchOperation.newBuilder()
      .addRecords(ManifestRecord.newBuilder()
        .setCurrentSsTable(nextSSTableIndex)
        .build()
      ).addRecords(ManifestRecord.newBuilder()
        .setCurrentWal(nextWalIndex)
        .build()
      ).addRecords(ManifestRecord.newBuilder()
        .setAddFile(AddFile.newBuilder()
          .addSsTableIndex(nextSSTableIndex)
          .build()
        ).build()
      ).build()
    manifest.commitChanges(batchOperation)
    walLogger.destroy()
    walLogger = LogWriter(config.dbPath, manifest)
    memTable.clear()
    openSSTables(config.dbPath)
  }

  private fun persistMemTableEntry(ssTableWriter: SSTableWriter) {
    for ((memTableKey, memTableValue) in memTable.getMemTableEntries()) {
      val entry = MemTableEntry.newBuilder()
        .setKey(memTableKey)
        .setValue(memTableValue)
        .build()
      ssTableWriter.add(entry)
    }
    ssTableWriter.finish()
  }
}