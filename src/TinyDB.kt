package src

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import com.google.common.primitives.Longs
import com.google.protobuf.ByteString
import src.comparator.MemTableKeyComparator
import src.config.Config
import src.manifest.Manifest
import src.memtable.MemTable
import src.proto.manifest.BatchOperation
import src.proto.manifest.ManifestRecord
import src.proto.manifest.RemoveFile
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableKeyRange
import src.proto.memtable.MemTableValue
import src.sstable.SSTableReader
import src.sstable.SSTableWriterHelper
import src.wal.LogReader
import src.wal.LogWriter
import java.io.Closeable
import java.nio.file.Path
import java.util.PriorityQueue
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.io.path.deleteIfExists

class TinyDB: Closeable {
  private val config: Config
  private val dbLock: DBLock
  private val manifest: Manifest
  private var walLogger: LogWriter
  private val memTable: MemTable
  private val openingSSTables: MutableMap<Long, SSTableReader>
  private var sequenceNumber: Long
  private val lock: ReentrantLock

  constructor(config: Config) {
    this.config = config
    this.lock = ReentrantLock()
    val dbPath = config.dbPath
    dbLock = DBLock(dbPath)
    manifest = Manifest(dbPath)
    memTable = MemTable()
    var currentBlockOffset = 0
    LogReader(dbPath, manifest).use { logReader ->
      var tmp = logReader.recover(memTable)
      sequenceNumber = tmp.first
      currentBlockOffset = tmp.second
    }
    walLogger = LogWriter(dbPath, manifest, currentBlockOffset)
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
      openingSSTables.remove(openingIndex)?.close()
    }
    for (fileIndex in listSSTableIndexes) {
      if (openingSSTables.containsKey(fileIndex)) {
        continue // Already opened
      }
      val ssTableReader = SSTableReader(dbPath, fileIndex)
      openingSSTables.put(fileIndex, ssTableReader)
    }
  }

  fun get(key: ByteArray): ByteArray? {
    return lock.withLock {
      val memTableKey = MemTableKey.newBuilder()
        .setKey(ByteString.copyFrom(key))
        .build()

      var memTableValue = memTable.get(memTableKey)
      if (memTableValue != null) {
        return@withLock when (memTableValue.dataCase) {
          MemTableValue.DataCase.VALUE -> memTableValue.value.toByteArray()
          else -> null
        }
      }

      for ((_, openingSSTable) in openingSSTables) {
        val entry = openingSSTable.get(memTableKey) ?: continue
        if (memTableValue == null || entry.value.sequence > memTableValue.sequence) {
          memTableValue = entry.value
        }
      }

      return@withLock memTableValue?.let {
        when (it.dataCase) {
          MemTableValue.DataCase.VALUE -> it.value.toByteArray()
          else -> null
        }
      }
    }
  }

  fun put(key: ByteArray, value: ByteArray) {
    return lock.withLock {
      val memTableKey = MemTableKey.newBuilder()
        .setKey(ByteString.copyFrom(key))
        .build()
      val memTableValue = MemTableValue.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .setSequence(sequenceNumber + 1)
        .build()
      return@withLock internalPut(memTableKey, memTableValue)
    }
  }

  fun delete(key: ByteArray) {
    return lock.withLock {
      val memTableKey = MemTableKey.newBuilder()
        .setKey(ByteString.copyFrom(key))
        .build()
      val memTableValue = MemTableValue.newBuilder()
        .setTombstone(true)
        .setSequence(sequenceNumber + 1)
        .build()
      return@withLock internalPut(memTableKey, memTableValue)
    }
  }

  override fun close() {
    manifest.close()
    walLogger.close()
    for ((_, ssTableReader) in openingSSTables) {
      ssTableReader.close()
    }
    dbLock.close()
  }

  @VisibleForTesting
  fun getLevelNFileCount(level: Long): Long {
    return openingSSTables.values.count { it.getLevel() == level }.toLong()
  }

  private fun internalPut(memTableKey: MemTableKey, memTableValue: MemTableValue) {
    walLogger.put(memTableKey, memTableValue)
    memTable.put(memTableKey, memTableValue)
    sequenceNumber += 1
    if (memTable.getEntriesCount() < config.memTableSizeLimit && memTable.getMemTableSize() < config.memTableSizeLimit) {
      return
    }
    writeMemTableToSSTable()
    runCompaction()
  }

  private fun writeMemTableToSSTable() {
    val tableWriterHelper = SSTableWriterHelper(
      config.dbPath,
      0L,
      config.ssTableBlockSize,
      config.ssTableRestartInterval,
      manifest.getCurrentSSTableIndex()
    )
    for ((memTableKey, memTableValue) in memTable.getMemTableEntries()) {
      tableWriterHelper.write(memTableKey, memTableValue)
    }
    val changes = tableWriterHelper.finish()
    val nextWalIndex = manifest.getCurrentWalIndex() + 1
    val finalChanges = changes.toBuilder()
      .addRecords(ManifestRecord.newBuilder().setCurrentWal(nextWalIndex))
      .build()
    manifest.commitChanges(finalChanges)
    walLogger.destroy()
    walLogger = LogWriter(config.dbPath, manifest, 0)
    memTable.clear()
    openSSTables(config.dbPath)
  }

  private fun runCompaction() {
    runCompactionLevel0()
    var level = 1L
    while (runCompactionAtLevel(level)) {
      level += 1L
    }
  }

  private fun runCompactionLevel0() {
    val levelOFiles = mutableSetOf<SSTableReader>()
    for ((_, ssTableReader) in openingSSTables) {
      if (ssTableReader.getLevel() == 0L) {
        levelOFiles.add(ssTableReader)
      }
    }
    if (levelOFiles.size < config.level0CompactionThreshold) {
      return
    }
    val compactionInputs = selectLevel0CompactionTables(levelOFiles)
    runCompaction(compactionInputs, 1L)
  }

  private fun runCompaction(compactionInputs: Set<SSTableReader>, targetLevel: Long) {
    val compactionResult = performCompaction(compactionInputs, targetLevel)
    manifest.commitChanges(compactionResult)
    val removeFiles = RemoveFile.newBuilder()
    for (file in compactionInputs) {
      removeFiles.addSsTableIndex(file.getSSTableIndex())
    }
    val deleteFileLog = BatchOperation.newBuilder()
      .addRecords(ManifestRecord.newBuilder().setRemoveFile(removeFiles).build())
      .build()
    manifest.commitChanges(deleteFileLog)
    for (file in compactionInputs) {
      val fileIndex = file.getSSTableIndex()
      openingSSTables.remove(fileIndex)?.close()
      Preconditions.checkArgument(
        config.dbPath.resolve("$fileIndex.sstable").deleteIfExists(),
        "Failed to delete SSTable file: $fileIndex.sstable"
      )
    }
  }

  private fun runCompactionAtLevel(level: Long): Boolean {
    val filesAtLevel = openingSSTables.values.filter { it.getLevel() == level }
    if (filesAtLevel.isEmpty()) {
      return false
    }
    val totalSize = filesAtLevel.sumOf { it.getDataSize() }
    val expectedSizeAtLevel = getDataSizeThresholdAtLevel(level)
    if (totalSize < expectedSizeAtLevel) {
      return true
    }

    val nextLevelFiles = openingSSTables.values.filter { it.getLevel() == level + 1L }.toSet()
    val fileScoresAtLevel = filesAtLevel.associateWith { file ->
      return@associateWith nextLevelFiles.count { nextFile ->
        return@count isRangeOverlapping(file.getKeyRange(), nextFile.getKeyRange())
      }
    }
    val sortedFiles = filesAtLevel
      .sortedWith(compareBy<SSTableReader> { fileScoresAtLevel[it]!! }
      .thenBy { it.getSSTableIndex() })

    var selectedSize = 0L
    val levelNFiles = mutableSetOf<SSTableReader>()
    for (file in sortedFiles) {
      levelNFiles.add(file)
      if (file.getLevel() != level) {
        continue
      }
      selectedSize += file.getDataSize()
      if (totalSize - selectedSize <= expectedSizeAtLevel) {
        break
      }
    }

    val compactionInputs = selectLevelNCompactionFiles(levelNFiles, nextLevelFiles)
    runCompaction(compactionInputs, level + 1L)
    return true
  }

  private fun selectLevelNCompactionFiles(selectedFiles: Set<SSTableReader>, nextLevelFiles: Set<SSTableReader>): Set<SSTableReader> {
    val compactionInputs = mutableSetOf<SSTableReader>()
    val keyRanges = calculateKeyRange(selectedFiles)
    compactionInputs.addAll(selectedFiles)
    for (nextLevelFile in nextLevelFiles) {
      if (isRangeOverlapping(keyRanges, nextLevelFile.getKeyRange())) {
        compactionInputs.add(nextLevelFile)
      }
    }
    return compactionInputs
  }

  private fun getDataSizeThresholdAtLevel(level: Long): Long {
    return when (level) {
      0L -> 8 * 1024 * 1024 // never reach this case
      1L -> 10 * 1024 * 1024 // 10MB (about 5 SSTables)
      2L -> 100L * 1024 * 1024 // 100MB (about 5 SSTables)
      3L -> 1000L * 1024 * 1024 // 1GB (about 5 SSTables)
      4L -> 10000L * 1024 * 1024 // 10GB (about 5 SSTables)
      else -> 100000L * 1024 * 1024 // 100GB+ (about 5 SSTables)
    }
  }

  private fun selectLevel0CompactionTables(level0Files: Set<SSTableReader>): Set<SSTableReader> {
    val level0Ranges = calculateKeyRange(level0Files)
    val level1Files = openingSSTables.values.filter { it.getLevel() == 1L }
    val overlappingFiles = level1Files.filter { file ->
      return@filter isRangeOverlapping(file.getKeyRange(), level0Ranges)
    }
    return level0Files + overlappingFiles
  }

  private fun calculateKeyRange(readers: Set<SSTableReader>): MemTableKeyRange {
    var smallest: MemTableKey? = null
    var largest: MemTableKey? = null
    for (reader in readers) {
      val minKey = reader.getKeyRange().startKey
      val maxKey = reader.getKeyRange().endKey
      if (smallest == null || MemTableKeyComparator.INSTANCE.compare(minKey, smallest) < 0) {
        smallest = minKey
      }
      if (largest == null || MemTableKeyComparator.INSTANCE.compare(maxKey, largest) > 0) {
        largest = maxKey
      }
    }

    return MemTableKeyRange.newBuilder()
      .setStartKey(smallest!!)
      .setEndKey(largest!!)
      .build()
  }

  private fun isRangeOverlapping(range1: MemTableKeyRange, range2: MemTableKeyRange): Boolean {
    return MemTableKeyComparator.INSTANCE.compare(range1.startKey, range2.endKey) <= 0 &&
           MemTableKeyComparator.INSTANCE.compare(range1.endKey, range2.startKey) >= 0
  }

  private fun performCompaction(compactionInputs: Set<SSTableReader>, targetLevel: Long): BatchOperation {
    val excludeReaders = openingSSTables.values.filter { table ->
      return@filter !compactionInputs.contains(table)
    }
    val priorityQueue = PriorityQueue(Comparator<Pair<MemTableEntry, Iterator<MemTableEntry>>> { p1, p2 ->
      val keyComparison = MemTableKeyComparator.INSTANCE.compare(p1.first.key, p2.first.key)
      if (keyComparison != 0) {
        return@Comparator keyComparison
      }
      return@Comparator Longs.compare(p1.first.value.sequence, p2.first.value.sequence)
    })
    for (reader in compactionInputs) {
      val iterator = reader.iterator()
      if (iterator.hasNext()) {
        priorityQueue.add(Pair(iterator.next(), iterator))
      }
    }

    val tableWriterHelper = SSTableWriterHelper(
      config.dbPath,
      targetLevel,
      config.ssTableBlockSize,
      config.ssTableRestartInterval,
      manifest.getCurrentSSTableIndex()
    )

    var lastKey: MemTableKey? = null
    var lastValue: MemTableValue? = null
    while (priorityQueue.isNotEmpty()) {
      val (currentEntry, iterator) = priorityQueue.poll()
      if (lastKey != currentEntry.key && lastValue != null && lastKey != null) {
        writeTableIfNeeded(tableWriterHelper, lastKey, lastValue, excludeReaders)
      }
      lastKey = currentEntry.key
      lastValue = currentEntry.value
      if (iterator.hasNext()) {
        priorityQueue.add(Pair(iterator.next(), iterator))
      }
    }
    if (lastValue != null && lastKey != null) {
      writeTableIfNeeded(tableWriterHelper, lastKey, lastValue, excludeReaders)
    }

    return tableWriterHelper.finish()
  }

  private fun writeTableIfNeeded(
    tableWriterHelper: SSTableWriterHelper,
    memTableKey: MemTableKey,
    memTableValue: MemTableValue,
    excludeReaders: List<SSTableReader>
  ) {
    for (reader in excludeReaders) {
      val value = reader.get(memTableKey) ?: continue
      if (value.value.sequence >= memTableValue.sequence) {
        return
      }
    }
    tableWriterHelper.write(memTableKey, memTableValue)
  }
}