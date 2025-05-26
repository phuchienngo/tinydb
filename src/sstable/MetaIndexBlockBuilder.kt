package src.sstable

import src.proto.sstable.BlockHandle
import src.proto.sstable.MetaIndexData

class MetaIndexBlockBuilder() {
  private val indexMap = MetaIndexData.newBuilder()
  fun add(key: String, blockHandle: BlockHandle) {
    indexMap.putMetaIndex(key, blockHandle)
  }

  fun finish(): ByteArray {
    return indexMap.build().toByteArray()
  }
}