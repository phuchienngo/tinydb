package src.sstable

import com.google.protobuf.ByteString
import src.proto.memtable.MemTableKey
import src.proto.sstable.BlockHandle
import src.proto.sstable.IndexData
import java.util.NavigableMap
import java.util.TreeMap

class IndexBlockReader {
  private val blockList: NavigableMap<MemTableKey, BlockHandle>
  private val comparator = Comparator<MemTableKey> { o1, o2 ->
    return@Comparator ByteString.unsignedLexicographicalComparator().compare(o1.key, o2.key)
  }

  constructor(data: ByteArray) {
    val indexData = IndexData.parseFrom(data)
    blockList = TreeMap(comparator)
    for (entry in indexData.entryList) {
      blockList[entry.lastKey] = entry.blockHandle
    }
  }

  fun getBlockHandle(lastKey: MemTableKey): BlockHandle? {
    return blockList[lastKey]
  }

  fun findBlockHandle(memTableKey: MemTableKey): BlockHandle? {
    return blockList.floorEntry(memTableKey)?.value
  }
}