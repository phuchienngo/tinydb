package src.wal

import com.google.common.hash.Hashing
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import src.manifest.Manifest
import src.memtable.MemTable
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import src.proto.wal.WALRecord
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.math.min

class WALogger: Closeable {
  companion object {
    private const val DATA_SIZE = 25
    private val LOG: Logger = LoggerFactory.getLogger(WALogger::class.java)
  }
  private val manifest: Manifest
  private val filePath: Path
  private var walSequenceNumber: Long
  private val fileChannel: FileChannel
  private val hashing = Hashing.crc32c()
  private val buffer = ByteBuffer.allocateDirect(32)

  constructor(dbPath: Path, manifest: Manifest) {
    this.manifest = manifest
    this.walSequenceNumber = manifest.committedWalIndex()
    this.filePath = dbPath.resolve("WAL-$walSequenceNumber")
    this.fileChannel = FileChannel.open(
      dbPath,
      StandardOpenOption.CREATE,
      StandardOpenOption.READ,
      StandardOpenOption.APPEND
    )
  }

  fun put(memTableKey: MemTableKey, memTableValue: MemTableValue, sequenceNumber: Long) {
    val record = WALRecord.newBuilder()
      .setKey(memTableKey)
      .setValue(memTableValue)
      .setOperation(WALRecord.Operation.PUT)
      .setSequenceNumber(sequenceNumber)
      .build()
    appendLog(record)
  }

  fun delete(memTableKey: MemTableKey, sequenceNumber: Long) {
    val record = WALRecord.newBuilder()
      .setKey(memTableKey)
      .setOperation(WALRecord.Operation.DELETE)
      .setSequenceNumber(sequenceNumber)
      .build()
    appendLog(record)
  }

  fun recover(memTable: MemTable) {
    fileChannel.position(0)
    buffer.clear()
    var temp = ByteBuffer.allocateDirect(1024)
    var totalSize = 0
    while (fileChannel.read(buffer) != -1) {
      buffer.flip()
      val blockRecord = BlockRecord.deserialize(buffer)
      validateChecksum(blockRecord)
      when (blockRecord.blockType) {
        BlockRecord.BlockType.FULL -> {
          val record = WALRecord.parseFrom(blockRecord.data
            .position(0)
            .limit(blockRecord.length.toInt())
          )
          processWALRecord(record, memTable)
        }
        BlockRecord.BlockType.FIRST -> {
          temp.clear()
          temp = ensureCapacity(temp, buffer.limit())
          totalSize += blockRecord.length
          temp.put(blockRecord.data)
        }
        BlockRecord.BlockType.MIDDLE -> {
          temp = ensureCapacity(temp, buffer.limit())
          totalSize += blockRecord.length
          temp.put(blockRecord.data)
        }
        BlockRecord.BlockType.LAST -> {
          temp = ensureCapacity(temp, buffer.limit())
          temp.put(blockRecord.data)
          totalSize += blockRecord.length
          temp.flip()
          val record = WALRecord.parseFrom(temp
            .position(0)
            .limit(totalSize)
          )
          totalSize = 0
          temp.clear()
          processWALRecord(record, memTable)
        }
      }
      buffer.clear()
    }
    buffer.clear()
    fileChannel.position(fileChannel.size())
  }

  private fun appendLog(walRecord: WALRecord) {
    val serializedLog = walRecord.toByteArray()
    val serializedSize = walRecord.serializedSize
    for (offset in 0 until serializedSize step DATA_SIZE) {
      buffer.clear()
      val length = min(DATA_SIZE, serializedLog.size - offset)
      val crc32 = calculateChecksum(serializedLog, offset, length)
      val blockType = getBlockType(offset, length)
      val data = ByteBuffer.wrap(serializedLog, offset, length)
      val record = BlockRecord(crc32, blockType, length.toShort(), data)
      record.serialize(buffer)
      while (buffer.position() < buffer.limit()) {
        buffer.put(0.toByte())
      }
      buffer.flip()
      fileChannel.write(buffer)
      fileChannel.force(true)
    }
  }

  override fun close() {
    fileChannel.force(true)
    fileChannel.close()
  }

  fun destroy() {
    fileChannel.close()
    Files.deleteIfExists(filePath)
  }

  private fun calculateChecksum(data: ByteArray, offset: Int, length: Int): Int {
    return hashing.hashBytes(data, offset, length).asInt()
  }

  private fun getBlockType(offset: Int, length: Int): BlockRecord.BlockType {
    return when {
      offset == 0 && length <= DATA_SIZE -> BlockRecord.BlockType.FULL
      offset == 0 && length > DATA_SIZE-> BlockRecord.BlockType.FIRST
      offset > 0 && length <= DATA_SIZE -> BlockRecord.BlockType.LAST
      else -> BlockRecord.BlockType.MIDDLE
    }
  }

  private fun ensureCapacity(currentBuffer: ByteBuffer, requestSize: Int): ByteBuffer {
    if (currentBuffer.capacity() < requestSize) {
      val newBuffer = ByteBuffer.allocateDirect(requestSize)
      newBuffer.put(currentBuffer)
      return newBuffer
    }
    return currentBuffer
  }

  private fun processWALRecord(record: WALRecord, memTable: MemTable) {
    when (record.operation) {
      WALRecord.Operation.PUT -> memTable.put(record.key.toByteArray(), record.value.toByteArray())
      WALRecord.Operation.DELETE -> memTable.delete(record.key.toByteArray())
      WALRecord.Operation.UNRECOGNIZED -> LOG.error("Unrecognized WAL operation")
    }
  }

  private fun validateChecksum(record: BlockRecord): Boolean {
    val serializedLog = record.data.array()
    val calculatedChecksum = calculateChecksum(serializedLog, 0, serializedLog.size)
    return calculatedChecksum == record.crc32
  }
}