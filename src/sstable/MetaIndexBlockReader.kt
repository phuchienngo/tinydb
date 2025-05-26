package src.sstable

import src.proto.sstable.BlockHandle
import src.proto.sstable.MetaIndexData

class MetaIndexBlockReader {
  private val indexMap: MetaIndexData

  constructor(data: ByteArray) {
    indexMap = MetaIndexData.parseFrom(data)
  }

  fun getBloomFilterBlockHandle(): BlockHandle {
    return indexMap.metaIndexMap["bloom_filter"]!!
  }

  fun getStatsBlockHandle(): BlockHandle {
    return indexMap.metaIndexMap["stats"]!!
  }
}