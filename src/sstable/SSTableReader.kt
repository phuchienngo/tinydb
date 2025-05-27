package src.sstable

import com.google.common.collect.Iterators
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.sstable.BlockHandle
import src.proto.sstable.PropertiesBlock
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path

class SSTableReader: Closeable, Iterable<MemTableEntry> {
  private val footer: Footer
  private val indexBlockReader: IndexBlockReader
  private val metaIndexBlockReader: MetaIndexBlockReader
  private val bloomFilterReader: BloomFilterReader
  private val fileChannel: FileChannel
  private val blockCache: MutableMap<BlockHandle, DataBlockReader>
  private val minKey: MemTableKey
  private val maxKey: MemTableKey
  private val dataSize: Long
  private val entries: Int
  private val ssTableIndex: Long
  private val level: Long

  constructor(dbPath: Path, ssTableIndex: Long) {
    fileChannel = FileChannel.open(
      dbPath.resolve("SSTABLE-$ssTableIndex"),
      java.nio.file.StandardOpenOption.READ
    )
    footer = readFooter()
    indexBlockReader = IndexBlockReader(readBlock(footer.indexHandle))
    metaIndexBlockReader = MetaIndexBlockReader(readBlock(footer.metaIndexHandle))
    val bloomFilterHandle = metaIndexBlockReader.getBloomFilterBlockHandle()
    bloomFilterReader = BloomFilterReader(readBlock(bloomFilterHandle))
    val propertiesHandle = metaIndexBlockReader.getStatsBlockHandle()
    val propertiesBlock = PropertiesBlock.parseFrom(readBlock(propertiesHandle))
    blockCache = mutableMapOf<BlockHandle, DataBlockReader>()
    minKey = propertiesBlock.minKey
    maxKey = propertiesBlock.maxKey
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

  private fun getOrLoadBlockHandle(blockHandle: BlockHandle): DataBlockReader {
    return blockCache.computeIfAbsent(blockHandle) {
      val dataBlock = readBlock(it)
      return@computeIfAbsent DataBlockReader(dataBlock)
    }
  }

  private fun readFooter(): Footer {
    fileChannel.position(fileChannel.size() - 40)
    val buffer = ByteBuffer.allocateDirect(40)
    fileChannel.read(buffer)
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
    val buffer = ByteBuffer.allocateDirect(handle.size.toInt())
    fileChannel.position(handle.offset)
    fileChannel.read(buffer)
    return buffer.array()
  }

  override fun close() {
    fileChannel.close()
    blockCache.clear()
  }

  override fun iterator(): Iterator<MemTableEntry> {
    val iterators = indexBlockReader.getBlockHandles()
      .map(::getOrLoadBlockHandle)
      .map(DataBlockReader::iterator)
    // TODO: Lazy loading of iterators
    return Iterators.concat(*iterators.toTypedArray())
  }
}