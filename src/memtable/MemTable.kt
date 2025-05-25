package src.memtable

import com.google.protobuf.ByteString
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

class MemTable {
  private val skipList = ConcurrentSkipListMap<MemTableKey, MemTableValue>()
  private val tableSize = AtomicLong(0)
  private val tombStone = MemTableValue.newBuilder()
    .setTombstone(true)
    .build()

  fun getMemTableSize(): Long {
    return tableSize.get()
  }

  fun get(key: ByteArray): ByteArray? {
    val key = MemTableKey.newBuilder().setKey(ByteString.copyFrom(key)).build()
    val value = skipList[key] ?: return null
    return if (value.tombstone) {
      null
    } else {
      value.value.toByteArray()
    }
  }

  fun put(key: ByteArray, value: ByteArray) {
    val key = MemTableKey.newBuilder().setKey(ByteString.copyFrom(key)).build()
    val value = MemTableValue.newBuilder().setValue(ByteString.copyFrom(value)).build()
    internalPut(key, value)
  }

  fun delete(key: ByteArray) {
    val key = MemTableKey.newBuilder().setKey(ByteString.copyFrom(key)).build()
    internalPut(key, tombStone)
  }

  private fun internalPut(memTableKey: MemTableKey, memTableValue: MemTableValue) {
    if (skipList.containsKey(memTableKey)) {
      val oldValue = skipList[memTableKey]!!
      tableSize.addAndGet(-memTableKey.key.size().toLong())
      tableSize.addAndGet(-oldValue.value.size().toLong())
    }
    skipList[memTableKey] = memTableValue
    tableSize.addAndGet(memTableKey.key.size().toLong())
    tableSize.addAndGet(memTableValue.value.size().toLong())
  }
}