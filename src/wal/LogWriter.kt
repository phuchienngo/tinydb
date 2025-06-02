package src.wal

import com.google.common.hash.Hashing
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import src.manifest.Manifest
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.math.min

class LogWriter: Closeable {
  companion object {
    private const val BLOCK_SIZE = 32768
    private const val HEADER_SIZE = 7
    private val LOG: Logger = LoggerFactory.getLogger(LogWriter::class.java)
  }
  private val manifest: Manifest
  private val filePath: Path
  private val randomAccessFile: RandomAccessFile
  private var currentBlockOffset = 0
  private val hashing = Hashing.crc32c()

  constructor(dbPath: Path, manifest: Manifest) {
    this.manifest = manifest
    val walSequenceNumber = manifest.getCurrentWalIndex()
    this.filePath = dbPath.resolve("${walSequenceNumber}.wal")
    this.randomAccessFile = RandomAccessFile(filePath.toFile(), "rws")
  }

  fun put(memTableKey: MemTableKey, memTableValue: MemTableValue) {
    val record = MemTableEntry.newBuilder()
      .setKey(memTableKey)
      .setValue(memTableValue)
      .build()
    appendLog(record)
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
    randomAccessFile.fd.sync()
  }

  private fun writeBlock(data: ByteArray, offset: Int, blockType: BlockRecord.BlockType): Int {
    val expectedSize = HEADER_SIZE + data.size - offset
    val writtenSize = min(expectedSize, BLOCK_SIZE - currentBlockOffset)
    val blockDataSize = writtenSize - HEADER_SIZE
    val bytes = ByteArray(writtenSize)
    val buffer = ByteBuffer.wrap(bytes)
    val crc32 = calculateChecksum(data, offset, blockDataSize)
    buffer.putInt(crc32)
    buffer.put(blockType.value)
    buffer.putShort(blockDataSize.toShort())
    buffer.put(data, offset, blockDataSize)
    buffer.flip()
    try {
      randomAccessFile.write(bytes)
      randomAccessFile.fd.sync()
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
    for (i in 0 until paddingSize) {
      randomAccessFile.writeByte(0)
    }
    randomAccessFile.fd.sync()
    return true
  }

  override fun close() {
    randomAccessFile.fd.sync()
    randomAccessFile.close()
  }

  fun destroy() {
    randomAccessFile.close()
    Files.deleteIfExists(filePath)
  }

  private fun calculateChecksum(data: ByteArray, offset: Int, length: Int): Int {
    return hashing.hashBytes(data, offset, length).asInt()
  }
}