package src.manifest

import com.google.common.base.Preconditions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import src.proto.manifest.BatchOperation
import src.proto.manifest.CompactManifest
import src.proto.manifest.ManifestRecord
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
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
  private lateinit var manifestChannel: FileChannel

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

  fun committedManifestIndex(): Long {
    return currentManifestIndex
  }

  fun committedSSTableIndex(): Long {
    return currentSSTableIndex
  }

  fun committedSSTableIndexes(): Set<Long> {
    return currentSSTables
  }

  fun commitChanges(batchOperation: BatchOperation) {
    val buffer = ByteBuffer.allocateDirect(4 + batchOperation.serializedSize)
    buffer.putInt(batchOperation.serializedSize)
    buffer.put(batchOperation.toByteArray())
    buffer.flip()
    try {
      while (buffer.hasRemaining()) {
        manifestChannel.write(buffer)
      }
      manifestChannel.force(true)
    } catch (e: Exception) {
      LOG.error("[commitChanges] Failed to commit changes to manifest", e)
      return
    }
    for (change in batchOperation.recordsList) {
      applyManifestRecord(change)
    }
    compactIfNeeded()
  }

  override fun close() {
    manifestChannel.close()
  }

  private fun initialize() {
    val current = dbPath.resolve("CURRENT")
    if (!Files.exists(current)) {
      LOG.debug("[initialize] Creating CURRENT file")
      Files.createFile(current)
      Files.createFile(dbPath.resolve("MANIFEST-0"))
      return
    }

    val channel = FileChannel.open(current, StandardOpenOption.READ)
    if (channel.size() == 0L) {
      LOG.debug("[initialize] CURRENT file is empty => ignoring")
      return
    }
    val buffer = ByteBuffer.allocateDirect(channel.size().toInt())
    channel.read(buffer)
    channel.close()
    buffer.flip()
    val currentManifestFile = StandardCharsets.US_ASCII.decode(buffer).toString()
    val regex = Regex.fromLiteral("MANIFEST-([0-9]+)")
    Preconditions.checkArgument(currentManifestFile.matches(regex), "Invalid CURRENT file format")
    currentManifestIndex = currentManifestFile.removePrefix("MANIFEST-").toLong()
    recoverManifestFiles()
  }

  private fun recoverManifestFiles() {
    val manifestFile = dbPath.resolve("MANIFEST-$currentManifestIndex")
    Preconditions.checkArgument(Files.exists(manifestFile), "Manifest file does not exist: $manifestFile")
    manifestChannel = FileChannel.open(manifestFile, StandardOpenOption.READ)
    val recordSizeBuffer = ByteBuffer.allocateDirect(4)
    var dataBuffer: ByteBuffer? = null
    while (manifestChannel.read(recordSizeBuffer) != -1) {
      recordSizeBuffer.flip()
      Preconditions.checkArgument(recordSizeBuffer.remaining() == 4, "Record size buffer must be 4 bytes")
      val recordSize = recordSizeBuffer.int
      recordSizeBuffer.clear()

      dataBuffer = ensureCapacity(dataBuffer, recordSize)
      dataBuffer.clear()
      dataBuffer.position(0).limit(recordSize)
      val readBytes = manifestChannel.read(dataBuffer)
      Preconditions.checkArgument(readBytes == recordSize, "Read size mismatch: expected $recordSize, got $readBytes")
      dataBuffer.flip()
      val record = ManifestRecord.parseFrom(dataBuffer)
      dataBuffer.clear()

      applyManifestRecord(record)
    }
    manifestChannel.close()
    manifestChannel = FileChannel.open(manifestFile, StandardOpenOption.APPEND)
  }

  private fun ensureCapacity(buffer: ByteBuffer?, size: Int): ByteBuffer {
    if (buffer != null && buffer.capacity() >= size) {
      return buffer
    }
    return ByteBuffer.allocateDirect(size)
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
    val manifestSize = manifestChannel.size()
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
    val buffer = ByteBuffer.allocateDirect(4 + compactManifest.serializedSize)
    buffer.putInt(compactManifest.serializedSize)
    buffer.put(compactManifest.toByteArray())
    buffer.flip()

    val newManifestChannel: FileChannel
    try {
      val newManifestFile = dbPath.resolve("MANIFEST-$newManifestIndex")
      newManifestChannel = FileChannel.open(
        newManifestFile,
        StandardOpenOption.APPEND,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
      )
      while (buffer.hasRemaining()) {
        newManifestChannel.write(buffer)
      }
      newManifestChannel.force(true)
    } catch (e: Exception) {
      LOG.error("[compactIfNeeded] Failed to create new manifest file", e)
      return
    }

    try {
      val currentFileChannel = FileChannel.open(
        dbPath.resolve("CURRENT"),
        StandardOpenOption.APPEND,
        StandardOpenOption.TRUNCATE_EXISTING
      )
      currentFileChannel.truncate(0)
      val buffer = StandardCharsets.US_ASCII.encode("MANIFEST-$newManifestIndex")
      while (buffer.hasRemaining()) {
        currentFileChannel.write(buffer)
      }
      currentFileChannel.force(true)
      currentFileChannel.close()
    } catch (e: Exception) {
      LOG.error("[compactIfNeeded] Failed to update CURRENT file", e)
      return
    }

    manifestChannel.force(true)
    manifestChannel.close()
    manifestChannel = newManifestChannel
    currentManifestIndex = newManifestIndex
    try {
      Files.delete(dbPath.resolve("MANIFEST-${newManifestIndex - 1}"))
    } catch (e: Exception) {
      LOG.error("[compactIfNeeded] Failed to delete old manifest file", e)
    }
  }
}