package src

import com.google.protobuf.ByteString
import org.apache.commons.collections4.map.MultiKeyMap
import src.manifest.Manifest
import src.memtable.MemTable
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import src.sstable.SSTableReader
import src.wal.WALogger
import java.io.Closeable
import java.nio.file.Path

class TinyDB: Closeable {
  private val dbLock: DBLock
  private val manifest: Manifest
  private var walLogger: WALogger
  private var memTable: MemTable
  private val openingSSTables: MultiKeyMap<Long, SSTableReader>
  private var sequenceNumber: Long

  constructor(dbPath: Path) {
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
      .setSequence(sequenceNumber + 1)
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

  }
}