package src.sstable

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink
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

  private class MemTableKeyFunnel: Funnel<MemTableKey> {
    override fun funnel(from: MemTableKey, into: PrimitiveSink) {
      into.putBytes(from.toByteArray())
    }
  }

  fun finish(): ByteArray {
    val output = ByteArrayOutputStream()
    bloomFilter.writeTo(output)
    return output.toByteArray()
  }
}