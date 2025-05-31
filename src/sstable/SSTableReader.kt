package src.sstable

import com.google.common.base.Preconditions
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableKeyRange
import src.proto.sstable.BlockHandle
import src.proto.sstable.PropertiesBlock
import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Path

class SSTableReader: Closeable, Iterable<MemTableEntry> {
  private val footer: Footer
  private val indexBlockReader: IndexBlockReader
  private val metaIndexBlockReader: MetaIndexBlockReader
  private val bloomFilterReader: BloomFilterReader
  private val randomAccessFile: RandomAccessFile
  private val blockCache: MutableMap<BlockHandle, DataBlockReader>
  private val keyRange: MemTableKeyRange
  private val dataSize: Long
  private val entries: Int
  private val ssTableIndex: Long
  private val level: Long

  constructor(dbPath: Path, ssTableIndex: Long) {
    val filePath = dbPath.resolve("${ssTableIndex}.sstable").toFile()
    randomAccessFile = RandomAccessFile(filePath, "r")
    footer = readFooter()
    indexBlockReader = IndexBlockReader(readBlock(footer.indexHandle))
    metaIndexBlockReader = MetaIndexBlockReader(readBlock(footer.metaIndexHandle))
    val bloomFilterHandle = metaIndexBlockReader.getBloomFilterBlockHandle()
    bloomFilterReader = BloomFilterReader(readBlock(bloomFilterHandle))
    val propertiesHandle = metaIndexBlockReader.getStatsBlockHandle()
    val propertiesBlock = PropertiesBlock.parseFrom(readBlock(propertiesHandle))
    blockCache = initBlockCache()
    keyRange = propertiesBlock.keyRange
    dataSize = propertiesBlock.dataSize
    entries = propertiesBlock.entries
    this.ssTableIndex = propertiesBlock.ssTableIndex
    this.level = propertiesBlock.level
  }

  fun get(memTableKey: MemTableKey): MemTableEntry? {
    if (!bloomFilterReader.mightContain(memTableKey)) {
      return null
    }
    val blockHandle = indexBlockReader.findBlockHandle(memTableKey)
    if (blockHandle == null) {
      return null
    }
    val dataBlockReader = getOrLoadBlockHandle(blockHandle)
    return dataBlockReader.get(memTableKey)
  }

  fun getLevel(): Long {
    return level
  }

  fun getSSTableIndex(): Long {
    return ssTableIndex
  }

  fun getKeyRange(): MemTableKeyRange {
    return keyRange
  }

  fun getDataSize(): Long {
    return dataSize
  }

  private fun getOrLoadBlockHandle(blockHandle: BlockHandle): DataBlockReader {
    return blockCache.computeIfAbsent(blockHandle) {
      val dataBlock = readBlock(it)
      return@computeIfAbsent DataBlockReader(dataBlock)
    }
  }

  private fun readFooter(): Footer {
    randomAccessFile.seek(randomAccessFile.length() - 40)
    val bytes = ByteArray(40)
    val readBytes = randomAccessFile.read(bytes)
    Preconditions.checkArgument(readBytes == 40, "Footer size mismatch")
    val buffer = ByteBuffer.wrap(bytes)
    val metaIndexHandle = BlockHandle.newBuilder()
      .setOffset(buffer.long)
      .setSize(buffer.long)
      .build()
    val indexHandle = BlockHandle.newBuilder()
      .setOffset(buffer.long)
      .setSize(buffer.long)
      .build()
    return Footer(metaIndexHandle, indexHandle)
  }

  private fun readBlock(handle: BlockHandle): ByteArray {
    val bytes = ByteArray(handle.size.toInt())
    randomAccessFile.seek(handle.offset)
    val readBytes = randomAccessFile.read(bytes)
    Preconditions.checkArgument(readBytes == bytes.size, "Block size mismatch")
    return bytes
  }

  override fun close() {
    randomAccessFile.close()
    blockCache.clear()
  }

  override fun iterator(): Iterator<MemTableEntry> {
    val blockHandles = indexBlockReader.getBlockHandles()
    val iterator = object: Iterator<MemTableEntry> {
      private var blockIndex = 0
      private var current: Iterator<MemTableEntry>? = null

      private fun loadNextBlock() {
        while (current?.hasNext() != true && blockIndex < blockHandles.size) {
          current = getOrLoadBlockHandle(blockHandles[blockIndex++]).iterator()
        }
      }

      override fun hasNext(): Boolean {
        loadNextBlock()
        return current != null && current!!.hasNext()
      }

      override fun next(): MemTableEntry {
        loadNextBlock()
        return current!!.next()
      }
    }
    return iterator
  }

  private fun initBlockCache(): MutableMap<BlockHandle, DataBlockReader> {
    return object: LinkedHashMap<BlockHandle, DataBlockReader>(128, 0.75f, true) {
      private val maxEntries = 128 // Set your cache size limit here
      override fun removeEldestEntry(eldest: Map.Entry<BlockHandle, DataBlockReader>): Boolean {
        return size > maxEntries
      }
    }
  }
}