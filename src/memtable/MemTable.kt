package src.memtable

import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

class MemTable {
  private val skipList = ConcurrentSkipListMap<MemTableKey, MemTableValue>()
  private val tableSize = AtomicLong(0)

  fun getMemTableSize(): Long {
    return tableSize.get()
  }

  fun getEntriesCount(): Int {
    return skipList.size
  }

  fun get(memTableKey: MemTableKey): MemTableValue? {
    return skipList[memTableKey]
  }

  fun put(memTableKey: MemTableKey, memTableValue: MemTableValue) {
    internalPut(memTableKey, memTableValue)
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