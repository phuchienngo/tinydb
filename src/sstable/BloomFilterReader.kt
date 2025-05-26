package src.sstable

import com.google.common.hash.BloomFilter
import src.proto.memtable.MemTableKey
import java.io.ByteArrayInputStream

@Suppress("UnstableApiUsage")
class BloomFilterReader {
  private val bloomFilter: BloomFilter<MemTableKey>
  constructor(data: ByteArray) {
    val inputStream = ByteArrayInputStream(data)
    bloomFilter = BloomFilter.readFrom(inputStream, MemTableKeyFunnel())
  }

  fun mightContain(memTableKey: MemTableKey): Boolean {
    return bloomFilter.mightContain(memTableKey)
  }
}