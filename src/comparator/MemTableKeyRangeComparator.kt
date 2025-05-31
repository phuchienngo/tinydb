package src.comparator

import src.proto.memtable.MemTableKeyRange

class MemTableKeyRangeComparator: Comparator<MemTableKeyRange> {
  companion object {
    val INSTANCE = MemTableKeyRangeComparator()
  }
  override fun compare(range1: MemTableKeyRange, range2: MemTableKeyRange): Int {
    val startComparison = MemTableKeyComparator.INSTANCE.compare(range1.startKey, range2.startKey)
    if (startComparison != 0) {
      return startComparison
    }
    return MemTableKeyComparator.INSTANCE.compare(range1.endKey, range2.endKey)
  }
}