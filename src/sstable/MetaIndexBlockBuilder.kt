package src.sstable

import src.proto.sstable.BlockHandle
import src.proto.sstable.MetaIndexData

class MetaIndexBlockBuilder {
  private val indexMap = MetaIndexData.newBuilder()

  fun addBloomFilterHandle(blockHandle: BlockHandle) {
    indexMap.setBloomFilter(blockHandle)
  }

  fun addStatsHandle(blockHandle: BlockHandle) {
    indexMap.setStats(blockHandle)
  }

  fun finish(): ByteArray {
    return indexMap.build().toByteArray()
  }
}