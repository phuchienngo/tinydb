package src.comparator

import com.google.protobuf.ByteString
import src.proto.memtable.MemTableKey

class MemTableKeyComparator: Comparator<MemTableKey> {
  companion object {
    val INSTANCE = MemTableKeyComparator()
  }
  override fun compare(o1: MemTableKey, o2: MemTableKey): Int {
    return ByteString.unsignedLexicographicalComparator().compare(o1.key, o2.key)
  }
}