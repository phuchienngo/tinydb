package src.sstable

import src.proto.memtable.MemTableEntry
import src.proto.sstable.BlockHandle
import src.proto.sstable.PropertiesBlock
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class SSTableWriter(
  dbPath: Path,
  ssTableIndex: Long,
  level: Int,
  private val blockSize: Int,
  restartInterval: Int
): Closeable {
  private val filePath = dbPath.resolve("SSTable-$level-$ssTableIndex")
  private val channel = FileChannel.open(
    filePath,
    StandardOpenOption.TRUNCATE_EXISTING,
    StandardOpenOption.CREATE,
    StandardOpenOption.APPEND
  )
  private val dataBlockBuilder = DataBlockBuilder(restartInterval, channel)
  private val indexBlockBuilder = IndexBlockBuilder()
  private val metaIndexBlockBuilder = MetaIndexBlockBuilder()
  private val bloomFilterBuilder = BloomFilterBuilder(100000, 0.01)
  private var recordCount = 0
  private var currentOffset = 0L

  fun add(memTableEntry: MemTableEntry) {
    val recordSize = estimateRecordSize(memTableEntry)
    val estimatedNextSize = dataBlockBuilder.writtenSize() + recordSize
    if (estimatedNextSize > blockSize) {
      flushDataBlock()
    }
    dataBlockBuilder.write(memTableEntry)
    bloomFilterBuilder.add(memTableEntry.key)
    recordCount += 1
  }

  fun finish(): Footer {
    flushDataBlock() // finish the last data block

    // properties
    val bloomFilter = bloomFilterBuilder.finish()
    val indexBlock = indexBlockBuilder.finish()
    val bloomFilterHandler = writeBlock(bloomFilter)
    val propertiesHandle = writePropertiesBlock(indexBlock.size)

    // meta index
    metaIndexBlockBuilder.add("bloom_filter", bloomFilterHandler)
    metaIndexBlockBuilder.add("stats", propertiesHandle)
    val metaIndexHandle = writeBlock(metaIndexBlockBuilder.finish())
    // index
    val indexHandle = writeBlock(indexBlock)
    // footer
    val footer = Footer(metaIndexHandle, indexHandle)
    val footerBuffer = footer.serialize()
    while (footerBuffer.hasRemaining()) {
      channel.write(footerBuffer)
    }
    channel.force(true)
    return footer
  }

  override fun close() {
    channel.force(true)
    channel.close()
  }

  private fun writePropertiesBlock(indexSize: Int): BlockHandle {
    val propertiesBlock = PropertiesBlock.newBuilder()
      .putProperties("entries", "$recordCount")
      .putProperties("dataSize", "$currentOffset")
      .putProperties("indexSize", "$indexSize")
      .build()
    return writeBlock(propertiesBlock.toByteArray())
  }

  private fun flushDataBlock() {
    val lastKey = dataBlockBuilder.lastKey()
    val blockHandle = BlockHandle.newBuilder()
      .setOffset(channel.position())
      .setSize(dataBlockBuilder.writtenSize().toLong())
      .build()
    indexBlockBuilder.add(lastKey, blockHandle)
    dataBlockBuilder.finish()
    dataBlockBuilder.reset()
    currentOffset = channel.position()
  }

  private fun writeBlock(data: ByteArray): BlockHandle {
    val offset = currentOffset
    val buffer = ByteBuffer.allocateDirect(data.size)
    buffer.put(data)
    buffer.flip()
    while (buffer.hasRemaining()) {
      channel.write(buffer)
    }
    channel.force(true)
    currentOffset += data.size
    return BlockHandle.newBuilder()
      .setSize(offset)
      .setSize(data.size.toLong())
      .build()
  }

  private fun estimateRecordSize(memTableEntry: MemTableEntry): Int {
    return memTableEntry.serializedSize + 4
  }
}
