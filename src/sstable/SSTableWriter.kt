package src.sstable

import com.google.protobuf.ByteString
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableKeyRange
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
  private val filePath = dbPath.resolve("${ssTableIndex}.sst")
  private val randomAccessFile = RandomAccessFile(filePath.toFile(), "rws")
  private val dataBlockWriter = DataBlockWriter(restartInterval, randomAccessFile)
  private val indexBlockBuilder = IndexBlockBuilder()
  private val metaIndexBlockBuilder = MetaIndexBlockBuilder()
  private val bloomFilterBuilder = BloomFilterBuilder(100000, 0.01)
  private var minKey: MemTableKey? = null
  private var maxKey: MemTableKey? = null
  private var recordCount = 0
  private var currentOffset = 0L

  fun add(memTableEntry: MemTableEntry) {
    val recordSize = estimateRecordSize(memTableEntry)
    val estimatedNextSize = dataBlockWriter.writtenSize() + recordSize
    if (estimatedNextSize > blockSize && dataBlockWriter.writtenSize() > 0) {
      setupNewDataBlock()
    }
    dataBlockWriter.write(memTableEntry)
    bloomFilterBuilder.add(memTableEntry.key)
    recordCount += 1
    minKey = if (minKey == null) {
      memTableEntry.key
    } else {
      minOf(minKey!!, memTableEntry.key)
    }
    maxKey = if (maxKey == null) {
      memTableEntry.key
    } else {
      maxOf(maxKey!!, memTableEntry.key)
    }
  }

  fun dataSize(): Long {
    return randomAccessFile.length()
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
    val keyRange = MemTableKeyRange.newBuilder()
      .setStartKey(minKey)
      .setEndKey(maxKey)
      .build()
    val propertiesBlock = PropertiesBlock.newBuilder()
      .setEntries(recordCount)
      .setDataSize(dataSize)
      .setSsTableIndex(ssTableIndex)
      .setLevel(level)
      .setIndexSize(indexSize.toLong())
      .setKeyRange(keyRange)
      .build()
    return writeBlock(propertiesBlock.toByteArray())
  }

  private fun setupNewDataBlock() {
    val lastKey = dataBlockWriter.lastKey()
    dataBlockWriter.finish()
    val writtenSize = dataBlockWriter.writtenSize().toLong()
    val blockHandle = BlockHandle.newBuilder()
      .setOffset(currentOffset)
      .setSize(writtenSize)
      .build()
    indexBlockBuilder.add(lastKey, blockHandle)
    dataBlockWriter.reset()
    currentOffset = randomAccessFile.filePointer
  }

  private fun writeBlock(data: ByteArray): BlockHandle {
    val offset = currentOffset
    randomAccessFile.write(data)
    currentOffset += data.size
    return BlockHandle.newBuilder()
      .setOffset(offset)
      .setSize(data.size.toLong())
      .build()
  }

  private fun estimateRecordSize(memTableEntry: MemTableEntry): Int {
    return memTableEntry.serializedSize + Int.SIZE_BYTES
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
