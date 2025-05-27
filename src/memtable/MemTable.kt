package src.memtable

import src.comparator.MemTableKeyComparator
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

class MemTable {
  private val skipList = ConcurrentSkipListMap<MemTableKey, MemTableValue>(MemTableKeyComparator.INSTANCE)
  private val tableSize = AtomicLong(0)

  fun getMemTableEntries(): Set<Map.Entry<MemTableKey, MemTableValue>> {
    return skipList.entries
  }

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

  fun clear() {
    skipList.clear()
    tableSize.set(0)
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