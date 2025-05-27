package src

import com.google.protobuf.ByteString
import org.apache.commons.collections4.map.MultiKeyMap
import src.config.Config
import src.manifest.Manifest
import src.memtable.MemTable
import src.proto.manifest.BatchOperation
import src.proto.manifest.ManifestRecord
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import src.sstable.SSTableReader
import src.sstable.SSTableWriter
import src.wal.WALogger
import java.io.Closeable

class TinyDB: Closeable {
  private val config: Config
  private val dbLock: DBLock
  private val manifest: Manifest
  private var walLogger: WALogger
  private val memTable: MemTable
  private val openingSSTables: MultiKeyMap<Long, SSTableReader>
  private var sequenceNumber: Long

  constructor(config: Config) {
    this.config = config
    val dbPath = config.dbPath
    dbLock = DBLock(dbPath)
    manifest = Manifest(dbPath)
    walLogger = WALogger(dbPath, manifest)
    memTable = MemTable()
    sequenceNumber = walLogger.recover(memTable)
    openingSSTables = MultiKeyMap()
    for (fileIndex in manifest.committedSSTableIndexes()) {
      val ssTableReader = SSTableReader(dbPath, fileIndex)
      val level = ssTableReader.getLevel()
      openingSSTables.put(level, fileIndex, ssTableReader)
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
      .addRecords(ManifestRecord.newBuilder().setCurrentSsTable(nextSSTableIndex).build())
      .addRecords(ManifestRecord.newBuilder().setCurrentWal(nextWalIndex).build())
      .build()
    manifest.commitChanges(batchOperation)
    walLogger.destroy()
    walLogger = WALogger(config.dbPath, manifest)
    memTable.clear()
    walLogger.recover(memTable)
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