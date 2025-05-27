package src.wal

import com.google.common.base.Preconditions
import com.google.common.hash.Hashing
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import src.manifest.Manifest
import src.memtable.MemTable
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.math.max
import kotlin.math.min

class WALogger: Closeable {
  companion object {
    private const val BLOCK_SIZE = 32768
    private const val HEADER_SIZE = 7
    private val LOG: Logger = LoggerFactory.getLogger(WALogger::class.java)
  }
  private val manifest: Manifest
  private val filePath: Path
  private var walSequenceNumber: Long
  private val fileChannel: FileChannel
  private var currentBlockOffset = 0
  private val hashing = Hashing.crc32c()

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

  fun put(memTableKey: MemTableKey, memTableValue: MemTableValue) {
    val record = MemTableEntry.newBuilder()
      .setKey(memTableKey)
      .setValue(memTableValue)
      .build()
    appendLog(record)
  }

  fun recover(memTable: MemTable): Long {
    fileChannel.position(0)
    val buffer = ByteBuffer.allocateDirect(BLOCK_SIZE)
    var temp: ByteBuffer? = null
    var logSequence = 0L
    while (fileChannel.read(buffer) != -1) {
      buffer.flip()
      parseBuffer@ while (buffer.hasRemaining()) {
        if (buffer.remaining() <= HEADER_SIZE) {
          // skip padding bytes
          break@parseBuffer
        }
        val crc32 = buffer.getInt()
        val blockType = BlockRecord.BlockType.fromValue(buffer.get())
        val blockSize = buffer.short.toInt()
        val data = ByteArray(blockSize)
        buffer.get(data)
        val record = BlockRecord(crc32, blockType, ByteBuffer.wrap(data))
        if (!validateChecksum(record)) {
          LOG.error("Invalid checksum for block, skipping")
          continue
        }
        when (blockType) {
          BlockRecord.BlockType.FULL -> {
            Preconditions.checkArgument(temp == null || temp.capacity() == 0, "Temp buffer should be empty for FULL block")
            logSequence = max(logSequence, processWALRecord(MemTableEntry.parseFrom(record.data), memTable))
          }
          BlockRecord.BlockType.FIRST -> {
            Preconditions.checkArgument(temp == null || temp.capacity() == 0, "Temp buffer should be empty for FIRST block")
            temp?.clear()
            temp = ensureCapacity(temp, (temp?.capacity() ?: 0) + record.data.capacity())
            temp.put(record.data)
          }
          BlockRecord.BlockType.MIDDLE -> {
            Preconditions.checkArgument(temp != null && temp.capacity() > 0, "Temp buffer should not be empty for MIDDLE block")
            temp = ensureCapacity(temp, temp!!.capacity() + record.data.capacity())
            temp.put(record.data)
          }
          BlockRecord.BlockType.LAST -> {
            Preconditions.checkArgument(temp != null && temp.capacity() > 0, "Temp buffer should not be empty for LAST block")
            temp = ensureCapacity(temp, temp!!.capacity() + record.data.capacity())
            temp.put(record.data)
            temp.flip()
            logSequence = max(logSequence, processWALRecord(MemTableEntry.parseFrom(temp), memTable))
            temp.clear()
          }
        }
      }
      buffer.clear()
    }
    fileChannel.position(fileChannel.size())
    return logSequence
  }

  private fun appendLog(memTableEntry: MemTableEntry) {
    val serializedLog = memTableEntry.toByteArray()
    val serializedSize = serializedLog.size
    if (serializedSize + HEADER_SIZE <= BLOCK_SIZE - currentBlockOffset) {
      currentBlockOffset += writeBlock(serializedLog, 0, BlockRecord.BlockType.FULL)
      if (paddingBlock()) {
        currentBlockOffset = 0
      }
      return
    }

    if (paddingBlock()) {
      currentBlockOffset = 0
    }

    if (serializedSize + HEADER_SIZE <= BLOCK_SIZE - currentBlockOffset) {
      currentBlockOffset += writeBlock(serializedLog, 0, BlockRecord.BlockType.FULL)
      return
    }

    var offset = writeBlock(serializedLog, 0, BlockRecord.BlockType.FIRST)
    currentBlockOffset += offset

    while (serializedSize - offset + HEADER_SIZE > BLOCK_SIZE - currentBlockOffset) {
      if (paddingBlock()) {
        currentBlockOffset = 0
      }
      val written = writeBlock(serializedLog, offset, BlockRecord.BlockType.MIDDLE)
      offset += written
      currentBlockOffset += written
    }

    if (paddingBlock()) {
      currentBlockOffset = 0
    }
    currentBlockOffset += writeBlock(serializedLog, offset, BlockRecord.BlockType.LAST)
    if (paddingBlock()) {
      currentBlockOffset = 0
    }
    fileChannel.force(true)
  }

  private fun writeBlock(data: ByteArray, offset: Int, blockType: BlockRecord.BlockType): Int {
    val expectedSize = HEADER_SIZE + data.size - offset
    val writtenSize = min(expectedSize, BLOCK_SIZE - currentBlockOffset)
    val blockDataSize = writtenSize - HEADER_SIZE
    val buffer = ByteBuffer.allocateDirect(writtenSize)
    val crc32 = calculateChecksum(data, offset, writtenSize)
    buffer.putInt(crc32)
    buffer.put(blockType.value)
    buffer.putShort(blockDataSize.toShort())
    buffer.put(data, offset, blockDataSize)
    buffer.flip()
    try {
      while (buffer.hasRemaining()) {
        fileChannel.write(buffer)
      }
      return blockDataSize
    } catch (e: Exception) {
      LOG.error("[writeBlock] Failed to write block to WAL", e)
      throw e
    }
  }

  private fun paddingBlock(): Boolean {
    if (BLOCK_SIZE - currentBlockOffset > HEADER_SIZE) {
      return false
    }
    val paddingSize = BLOCK_SIZE - currentBlockOffset
    val buffer = ByteBuffer.allocateDirect(paddingSize)
    val b = 0.toByte()
    while (buffer.hasRemaining()) {
      buffer.put(b)
    }
    buffer.flip()
    fileChannel.write(buffer)
    fileChannel.force(true)
    return true
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

  private fun ensureCapacity(currentBuffer: ByteBuffer?, requestSize: Int): ByteBuffer {
    if (currentBuffer == null || currentBuffer.capacity() < requestSize) {
      val newBuffer = ByteBuffer.allocateDirect(requestSize)
      currentBuffer?.flip() ?: return newBuffer
      newBuffer.put(currentBuffer)
      return newBuffer
    }
    return currentBuffer
  }

  private fun processWALRecord(memTableEntry: MemTableEntry, memTable: MemTable): Long {
    memTable.put(memTableEntry.key, memTableEntry.value)
    return memTableEntry.value.sequence
  }

  private fun validateChecksum(record: BlockRecord): Boolean {
    val serializedLog = record.data.array()
    val calculatedChecksum = calculateChecksum(serializedLog, 0, serializedLog.size)
    return calculatedChecksum == record.crc32
  }
}