package src.sstable

import src.comparator.MemTableKeyComparator
import src.proto.memtable.MemTableKey
import src.proto.sstable.BlockHandle
import src.proto.sstable.IndexData
import java.util.NavigableMap
import java.util.TreeMap

class IndexBlockReader {
  private val blockList: NavigableMap<MemTableKey, BlockHandle>

  constructor(data: ByteArray) {
    val indexData = IndexData.parseFrom(data)
    blockList = TreeMap(MemTableKeyComparator.INSTANCE)
    for (entry in indexData.entryList) {
      blockList[entry.lastKey] = entry.blockHandle
    }
  }

  fun getBlockHandles(): List<BlockHandle> {
    return blockList.values.toList()
  }

  fun findBlockHandle(memTableKey: MemTableKey): BlockHandle? {
    return blockList.ceilingEntry(memTableKey)?.value
  }
}