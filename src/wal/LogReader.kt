package src.wal

import com.google.common.base.Preconditions
import com.google.common.hash.Hashing
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import src.manifest.Manifest
import src.memtable.MemTable
import src.proto.memtable.MemTableEntry
import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.math.max

class LogReader: Closeable {
  companion object {
    private const val BLOCK_SIZE = 32768
    private const val HEADER_SIZE = 7
    private val LOG: Logger = LoggerFactory.getLogger(LogWriter::class.java)
  }
  private val hashing = Hashing.crc32c()
  private val randomAccessFile: RandomAccessFile
  constructor(dbPath: Path, manifest: Manifest) {
    val walSequenceNumber = manifest.getCurrentWalIndex()
    val filePath = dbPath.resolve("${walSequenceNumber}.wal")
    if (!filePath.exists()) {
      Files.createFile(filePath)
    }
    randomAccessFile = RandomAccessFile(filePath.toFile(), "r")
  }

  fun recover(memTable: MemTable): Pair<Long, Int> {
    randomAccessFile.seek(0)
    val block = ByteArray(BLOCK_SIZE)
    var logSequence = 0L
    var temp: ByteBuffer? = null
    var currentBlockOffset = 0
    while (randomAccessFile.filePointer < randomAccessFile.length()) {
      currentBlockOffset = 0
      val bytesRead = randomAccessFile.read(block)
//      Preconditions.checkArgument(bytesRead == BLOCK_SIZE, "Expected to read full block size, but got $bytesRead")
      val buffer = ByteBuffer.wrap(block)
      buffer.limit(bytesRead)
      buffer.position(0)
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
        currentBlockOffset += HEADER_SIZE + blockSize
        val record = BlockRecord(crc32, blockType, ByteBuffer.wrap(data))
        if (!validateChecksum(record)) {
          LOG.error("Invalid checksum for block, skipping")
          continue
        }
        when (blockType) {
          BlockRecord.BlockType.FULL -> {
            Preconditions.checkArgument(temp == null || temp.limit() == 0, "Temp buffer should be empty for FULL block")
            logSequence = max(logSequence, processWALRecord(MemTableEntry.parseFrom(record.data), memTable))
          }
          BlockRecord.BlockType.FIRST -> {
            Preconditions.checkArgument(temp == null || temp.position() == 0, "Temp buffer should be empty for FIRST block")
            temp?.position(0)
            temp = ensureCapacity(temp, (temp?.position() ?: 0) + record.data.capacity())
            temp.put(record.data)
          }
          BlockRecord.BlockType.MIDDLE -> {
            Preconditions.checkArgument(temp != null && temp.position() > 0, "Temp buffer should not be empty for MIDDLE block")
            temp = ensureCapacity(temp, temp!!.position() + record.data.capacity())
            temp.put(record.data)
          }
          BlockRecord.BlockType.LAST -> {
            Preconditions.checkArgument(temp != null && temp.position() > 0, "Temp buffer should not be empty for LAST block")
            temp = ensureCapacity(temp, temp!!.position() + record.data.capacity())
            temp.put(record.data)
            temp.flip()
            logSequence = max(logSequence, processWALRecord(MemTableEntry.parseFrom(temp), memTable))
            temp.clear().limit(0)
          }
        }
      }
    }
    return Pair(logSequence, currentBlockOffset % BLOCK_SIZE)
  }

  private fun ensureCapacity(currentBuffer: ByteBuffer?, requestSize: Int): ByteBuffer {
    if (currentBuffer == null || currentBuffer.capacity() < requestSize) {
      val newBuffer = ByteBuffer.allocateDirect(requestSize)
      newBuffer.limit(requestSize)
      currentBuffer?.flip() ?: return newBuffer
      newBuffer.put(currentBuffer)
      return newBuffer
    }
    currentBuffer.limit(currentBuffer.capacity())
    return currentBuffer
  }

  private fun processWALRecord(memTableEntry: MemTableEntry, memTable: MemTable): Long {
    memTable.put(memTableEntry.key, memTableEntry.value)
    return memTableEntry.value.sequence
  }

  private fun validateChecksum(record: BlockRecord): Boolean {
    val serializedLog = record.data.array()
    val calculatedChecksum = calculateChecksum(serializedLog, serializedLog.size)
    return calculatedChecksum == record.crc32
  }

  private fun calculateChecksum(data: ByteArray, length: Int): Int {
    return hashing.hashBytes(data, 0, length).asInt()
  }

  override fun close() {
    randomAccessFile.close()
  }
}