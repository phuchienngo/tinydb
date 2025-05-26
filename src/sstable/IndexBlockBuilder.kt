package src.sstable

import src.proto.memtable.MemTableKey
import src.proto.sstable.BlockHandle
import src.proto.sstable.IndexData

class IndexBlockBuilder() {
  private val blockList = IndexData.newBuilder()

  fun add(lastKey: MemTableKey, blockHandle: BlockHandle) {
    val entry = IndexData.Entry.newBuilder()
      .setLastKey(lastKey)
      .setBlockHandle(blockHandle)
      .build()
    blockList.addEntry(entry)
  }

  fun finish(): ByteArray {
    return blockList.build().toByteArray()
  }
}