package src.sstable

import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink
import src.proto.memtable.MemTableKey

@Suppress("UnstableApiUsage")
class MemTableKeyFunnel: Funnel<MemTableKey> {
  override fun funnel(from: MemTableKey, into: PrimitiveSink) {
    into.putBytes(from.toByteArray())
  }
}