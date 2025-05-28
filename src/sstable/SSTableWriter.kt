package src.sstable

import com.google.protobuf.ByteString
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.sstable.BlockHandle
import src.proto.sstable.PropertiesBlock
import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.file.Path

class SSTableWriter(
  dbPath: Path,
  private val ssTableIndex: Long,
  private val level: Long,
  private val blockSize: Int,
  restartInterval: Int
): Closeable {
  private val filePath = dbPath.resolve("${ssTableIndex}.sstable")
  private val randomAccessFile = RandomAccessFile(filePath.toFile(), "w")
  private val dataBlockWriter = DataBlockWriter(restartInterval, randomAccessFile)
  private val indexBlockBuilder = IndexBlockBuilder()
  private val metaIndexBlockBuilder = MetaIndexBlockBuilder()
  private val bloomFilterBuilder = BloomFilterBuilder(100000, 0.01)
  private var minKey = MemTableKey.getDefaultInstance()
  private var maxKey = MemTableKey.getDefaultInstance()
  private var recordCount = 0
  private var currentOffset = 0L

  fun add(memTableEntry: MemTableEntry) {
    val recordSize = estimateRecordSize(memTableEntry)
    val estimatedNextSize = dataBlockWriter.writtenSize() + recordSize
    if (estimatedNextSize > blockSize) {
      setupNewDataBlock()
    }
    dataBlockWriter.write(memTableEntry)
    bloomFilterBuilder.add(memTableEntry.key)
    recordCount += 1
    minKey = minOf(minKey, memTableEntry.key)
    maxKey = maxOf(maxKey, memTableEntry.key)
  }

  fun finish(): Footer {
    setupNewDataBlock() // finish the last data block

    // properties
    val bloomFilter = bloomFilterBuilder.finish()
    val indexBlock = indexBlockBuilder.finish()
    val dataSize = currentOffset
    val bloomFilterHandler = writeBlock(bloomFilter)
    val propertiesHandle = writePropertiesBlock(indexBlock.size, dataSize)

    // meta index
    metaIndexBlockBuilder.addBloomFilterHandle(bloomFilterHandler)
    metaIndexBlockBuilder.addStatsHandle(propertiesHandle)
    val metaIndexHandle = writeBlock(metaIndexBlockBuilder.finish())
    // index
    val indexHandle = writeBlock(indexBlock)
    // footer
    val footer = Footer(metaIndexHandle, indexHandle)
    val footerBuffer = footer.serialize()
    randomAccessFile.write(footerBuffer)
    randomAccessFile.fd.sync()
    return footer
  }

  override fun close() {
    randomAccessFile.fd.sync()
    randomAccessFile.close()
  }

  private fun writePropertiesBlock(indexSize: Int, dataSize: Long): BlockHandle {
    val propertiesBlock = PropertiesBlock.newBuilder()
      .setEntries(recordCount)
      .setDataSize(dataSize)
      .setSsTableIndex(ssTableIndex)
      .setLevel(level)
      .setIndexSize(indexSize.toLong())
      .setMinKey(minKey)
      .setMaxKey(maxKey)
      .build()
    return writeBlock(propertiesBlock.toByteArray())
  }

  private fun setupNewDataBlock() {
    val lastKey = dataBlockWriter.lastKey()
    val blockHandle = BlockHandle.newBuilder()
      .setOffset(randomAccessFile.filePointer)
      .setSize(dataBlockWriter.writtenSize().toLong())
      .build()
    indexBlockBuilder.add(lastKey, blockHandle)
    dataBlockWriter.finish()
    dataBlockWriter.reset()
    currentOffset = randomAccessFile.filePointer
  }

  private fun writeBlock(data: ByteArray): BlockHandle {
    val offset = currentOffset
    randomAccessFile.write(data)
    randomAccessFile.fd.sync()
    currentOffset += data.size
    return BlockHandle.newBuilder()
      .setSize(offset)
      .setSize(data.size.toLong())
      .build()
  }

  private fun estimateRecordSize(memTableEntry: MemTableEntry): Int {
    return memTableEntry.serializedSize + 4
  }

  private fun minOf(a: MemTableKey, b: MemTableKey): MemTableKey {
    val comparison = ByteString.unsignedLexicographicalComparator().compare(a.key, b.key)
    return if (comparison <= 0) a else b
  }

  private fun maxOf(a: MemTableKey, b: MemTableKey): MemTableKey {
    val comparison = ByteString.unsignedLexicographicalComparator().compare(a.key, b.key)
    return if (comparison <= 0) b else a
  }
}
