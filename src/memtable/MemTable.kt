package src.memtable

import src.comparator.MemTableKeyComparator
import src.proto.memtable.MemTableKey
import src.proto.memtable.MemTableValue
import java.util.TreeMap

class MemTable {
  private val treeMap = TreeMap<MemTableKey, MemTableValue>(MemTableKeyComparator.INSTANCE)
  private var tableSize = 0L

  fun getMemTableEntries(): Set<Map.Entry<MemTableKey, MemTableValue>> {
    return treeMap.entries
  }

  fun getMemTableSize(): Long {
    return tableSize
  }

  fun getEntriesCount(): Int {
    return treeMap.size
  }

  fun get(memTableKey: MemTableKey): MemTableValue? {
    return treeMap[memTableKey]
  }

  fun put(memTableKey: MemTableKey, memTableValue: MemTableValue) {
    internalPut(memTableKey, memTableValue)
  }

  fun clear() {
    treeMap.clear()
    tableSize = 0L
  }

  private fun internalPut(memTableKey: MemTableKey, memTableValue: MemTableValue) {
    if (treeMap.containsKey(memTableKey)) {
      val oldValue = treeMap[memTableKey]!!
      tableSize += -memTableKey.key.size().toLong()
      tableSize += -oldValue.value.size().toLong()
    }
    treeMap[memTableKey] = memTableValue
    tableSize += memTableKey.key.size().toLong()
    tableSize += memTableValue.value.size().toLong()
  }
}