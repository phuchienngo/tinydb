package src.sstable

import src.proto.sstable.BlockHandle
import src.proto.sstable.MetaIndexData

class MetaIndexBlockBuilder {
  private val indexMap = MetaIndexData.newBuilder()

  fun addBloomFilterHandle(blockHandle: BlockHandle) {
    indexMap.putMetaIndex("bloom_filter", blockHandle)
  }

  fun addStatsHandle(blockHandle: BlockHandle) {
    indexMap.putMetaIndex("stats", blockHandle)
  }

  fun finish(): ByteArray {
    return indexMap.build().toByteArray()
  }
}