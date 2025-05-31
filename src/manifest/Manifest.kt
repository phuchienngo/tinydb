package src.manifest

import com.google.common.base.Preconditions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import src.proto.manifest.BatchOperation
import src.proto.manifest.CompactManifest
import src.proto.manifest.ManifestRecord
import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createFile
import kotlin.io.path.exists
import kotlin.math.max

class Manifest: Closeable {
  companion object {
    private const val MANIFEST_SIZE_LIMIT = 1024
    private val LOG: Logger = LoggerFactory.getLogger(Manifest::class.java)
  }
  private val dbPath: Path
  private val currentSSTables: MutableSet<Long>
  private var currentSSTableIndex: Long
  private var currentWalIndex: Long
  private var currentManifestIndex: Long
  private lateinit var randomAccessFile: RandomAccessFile

  constructor(dbPath: Path) {
    this.dbPath = dbPath
    currentSSTables = mutableSetOf()
    currentWalIndex = 0
    currentManifestIndex = 0
    currentSSTableIndex = 0
    initialize()
  }

  fun committedWalIndex(): Long {
    return currentWalIndex
  }

  fun committedSSTableIndex(): Long {
    return currentSSTableIndex
  }

  fun committedSSTableIndexes(): Set<Long> {
    return currentSSTables
  }

  fun commitChanges(batchOperation: BatchOperation) {
    val serializedSize = batchOperation.serializedSize
    val byteArray = batchOperation.toByteArray()
    try {
      randomAccessFile.write(serializedSize)
      randomAccessFile.write(byteArray)
      randomAccessFile.fd.sync()
    } catch (e: Exception) {
      LOG.error("[commitChanges] Failed to commit changes to manifest", e)
      throw e
    }
    for (change in batchOperation.recordsList) {
      applyManifestRecord(change)
    }
    compactIfNeeded()
  }

  override fun close() {
    randomAccessFile.fd.sync()
    randomAccessFile.close()
  }

  private fun initialize() {
    val current = dbPath.resolve("CURRENT")
    if (!Files.exists(current)) {
      LOG.debug("[initialize] Creating CURRENT file")
      Files.createFile(current)
      Files.createFile(dbPath.resolve("0.manifest"))
      return
    }

    val currentRandomAccessFile = RandomAccessFile(current.toFile(), "r")
    if (currentRandomAccessFile.length() == 0L) {
      LOG.debug("[initialize] CURRENT file is empty => ignoring")
      return
    }
    val bytes = ByteArray(currentRandomAccessFile.length().toInt())
    val readBytes = currentRandomAccessFile.read(bytes)
    Preconditions.checkArgument(readBytes == bytes.size, "Failed to read CURRENT file")
    val currentManifestFile = StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(bytes)).toString()
    val regex = Regex("\\d+\\.manifest")
    Preconditions.checkArgument(currentManifestFile.matches(regex), "Invalid CURRENT file format")
    currentManifestIndex = currentManifestFile.substring(0, currentManifestFile.indexOf(".")).toLong()
    recoverManifestFiles()
  }

  private fun recoverManifestFiles() {
    val manifestFile = dbPath.resolve("${currentManifestIndex}.manifest").toFile()
    Preconditions.checkArgument(manifestFile.exists(), "Manifest file does not exist: $manifestFile")
    randomAccessFile = RandomAccessFile(manifestFile, "rw")
    randomAccessFile.seek(0)
    while (randomAccessFile.filePointer < randomAccessFile.length()) {
      val recordSize = randomAccessFile.readInt()
      Preconditions.checkArgument(recordSize >= 0, "Record size must be non-negative: $recordSize")
      val bytes = ByteArray(recordSize)
      val readBytes = randomAccessFile.read(bytes)
      Preconditions.checkArgument(readBytes == recordSize, "Read size mismatch: expected $recordSize, got $readBytes")
      val record = ManifestRecord.parseFrom(bytes)
      applyManifestRecord(record)
    }
    randomAccessFile.seek(randomAccessFile.length())
  }

  private fun applyManifestRecord(record: ManifestRecord) {
    when (record.recordCase) {
      ManifestRecord.RecordCase.ADD_FILE -> {
        val changes = record.addFile
        for (fileIndex in changes.ssTableIndexList) {
          currentSSTables.add(fileIndex)
          currentSSTableIndex = max(currentSSTableIndex, fileIndex)
        }
      }
      ManifestRecord.RecordCase.REMOVE_FILE -> {
        val changes = record.removeFile
        for (fileIndex in changes.ssTableIndexList) {
          currentSSTables.remove(fileIndex)
        }
      }
      ManifestRecord.RecordCase.COMPACT_MANIFEST -> {
        currentSSTables.clear()
        currentWalIndex = record.compactManifest.currentWal
        currentManifestIndex = record.compactManifest.currentManifest
        currentSSTableIndex = record.currentSsTable
        for (fileIndex in record.compactManifest.ssTableIndexList) {
          currentSSTables.add(fileIndex)
        }
      }
      ManifestRecord.RecordCase.BATCH_OPERATION -> {
        val changes = record.batchOperation
        for (change in changes.recordsList) {
          applyManifestRecord(change)
        }
      }
      ManifestRecord.RecordCase.CURRENT_WAL -> {
        currentWalIndex = record.currentWal
      }
      ManifestRecord.RecordCase.CURRENT_MANIFEST -> {
        currentManifestIndex = record.currentManifest
      }
      ManifestRecord.RecordCase.CURRENT_SS_TABLE -> {
        currentSSTableIndex = record.currentSsTable
      }
      ManifestRecord.RecordCase.RECORD_NOT_SET -> {
        throw IllegalStateException("Record not set in manifest record")
      }
    }
  }

  private fun compactIfNeeded() {
    val manifestSize = randomAccessFile.length()
    if (manifestSize <= MANIFEST_SIZE_LIMIT) {
      return
    }
    val newManifestIndex = currentManifestIndex + 1
    val compactManifest = CompactManifest.newBuilder()
      .setCurrentManifest(newManifestIndex)
      .setCurrentWal(currentWalIndex)
      .setCurrentSsTable(currentSSTableIndex)
      .addAllSsTableIndex(currentSSTables)
      .build()
    val serializedSize = compactManifest.serializedSize
    val bytes = compactManifest.toByteArray()

    val newRandomAccessFile: RandomAccessFile
    try {
      val newManifestFile = dbPath.resolve("${newManifestIndex}.manifest")
      if (!newManifestFile.exists()) {
        newManifestFile.createFile()
      }
      newRandomAccessFile = RandomAccessFile(newManifestFile.toFile(), "rw")
      newRandomAccessFile.setLength(0)
      newRandomAccessFile.seek(0)
      newRandomAccessFile.writeInt(serializedSize)
      newRandomAccessFile.write(bytes)
      newRandomAccessFile.fd.sync()
    } catch (e: Exception) {
      LOG.error("[compactIfNeeded] Failed to create new manifest file", e)
      throw e
    }

    try {
      val currentFile = dbPath.resolve("CURRENT").toFile()
      if (!currentFile.exists()) {
        currentFile.createNewFile()
      }
      val currentRandomAccessFile = RandomAccessFile(currentFile, "w")
      currentRandomAccessFile.setLength(0)
      currentRandomAccessFile.seek(0)
      currentRandomAccessFile.write(StandardCharsets.US_ASCII.encode("${newManifestIndex}.manifest").array())
      currentRandomAccessFile.fd.sync()
      currentRandomAccessFile.close()
    } catch (e: Exception) {
      LOG.error("[compactIfNeeded] Failed to update CURRENT file", e)
      return
    }

    randomAccessFile.fd.sync()
    randomAccessFile.close()
    randomAccessFile = newRandomAccessFile
    currentManifestIndex = newManifestIndex
    try {
      Files.delete(dbPath.resolve("${newManifestIndex - 1}.manifest"))
    } catch (e: Exception) {
      LOG.error("[compactIfNeeded] Failed to delete old manifest file", e)
    }
  }
}