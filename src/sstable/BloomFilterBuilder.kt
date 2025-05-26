package src.sstable

import com.google.common.hash.BloomFilter
import src.proto.memtable.MemTableKey
import java.io.ByteArrayOutputStream

@Suppress("UnstableApiUsage")
class BloomFilterBuilder(
  expectedInsertions: Int,
  fpp: Double
) {
  private val bloomFilter = BloomFilter.create(
    MemTableKeyFunnel(),
    expectedInsertions,
    fpp
  )

  fun add(memTableKey: MemTableKey) {
    bloomFilter.put(memTableKey)
  }

  fun finish(): ByteArray {
    val output = ByteArrayOutputStream()
    bloomFilter.writeTo(output)
    return output.toByteArray()
  }
}