package src.sstable

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import src.proto.sstable.BlockHandle
import src.proto.sstable.PropertiesBlock
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path

class SSTableReader: Closeable {
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

  constructor(dbPath: Path, ssTableIndex: Long, level: Long) {
    fileChannel = FileChannel.open(
      dbPath.resolve("SSTABLE-$level-$ssTableIndex"),
      java.nio.file.StandardOpenOption.READ
    )
    footer = readFooter()
    indexBlockReader = IndexBlockReader(readBlock(footer.indexHandle))
    metaIndexBlockReader = MetaIndexBlockReader(readBlock(footer.metaIndexHandle))
    val bloomFilterHandle = metaIndexBlockReader.getBloomFilterBlockHandle()
    bloomFilterReader = BloomFilterReader(readBlock(bloomFilterHandle))
    val propertiesHandle = metaIndexBlockReader.getStatsBlockHandle()
    val propertiesBlock = PropertiesBlock.parseFrom(readBlock(propertiesHandle))
    val properties = propertiesBlock.propertiesMap
    blockCache = mutableMapOf<BlockHandle, DataBlockReader>()
    minKey = MemTableKey.parseFrom(properties["minKey"])
    maxKey = MemTableKey.parseFrom(properties["maxKey"])
    dataSize = Longs.fromByteArray(properties["dataSize"]!!.toByteArray())
    entries = Ints.fromByteArray(properties["entries"]!!.toByteArray())
  }

  fun get(memTableKey: MemTableKey): MemTableEntry? {
    if (!bloomFilterReader.mightContain(memTableKey)) {
      return null
    }
    val blockHandle = indexBlockReader.findBlockHandle(memTableKey)
    if (blockHandle == null) {
      return null
    }
    val dataBlockReader = if (blockCache.containsKey(blockHandle)) {
      blockCache[blockHandle]!!
    } else {
      val dataBlock = readBlock(blockHandle)
      val blockReader = DataBlockReader(dataBlock)
      blockCache[blockHandle] = blockReader
      blockReader
    }
    return dataBlockReader.get(memTableKey)
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
  }
}