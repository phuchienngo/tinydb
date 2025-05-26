package src.sstable

import com.google.protobuf.ByteString
import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import java.nio.ByteBuffer

class DataBlockReader {
  private val blockData: ByteArray
  private val dataSize: Int
  private val restartPoints: List<Int>

  constructor(blockData: ByteArray) {
    this.blockData = blockData
    val buffer = ByteBuffer.wrap(blockData)
    buffer.position(blockData.size - 4)
    val restartCount = buffer.int
    dataSize = blockData.size - 4 - restartCount * 4
    restartPoints = mutableListOf()
    buffer.position(dataSize)
    for (i in 0 until restartCount) {
      restartPoints.add(buffer.int)
    }
    restartPoints.sort()
  }

  fun get(memTableKey: MemTableKey): MemTableEntry? {
    var left = 0
    var right = restartPoints.size - 1
    var result = 0
    while (left <= right) {
      val mid = left + (right - left) / 2
      val entry = getEntryAtOffset(restartPoints[mid])
      val cmp = ByteString.unsignedLexicographicalComparator().compare(entry.key.key, memTableKey.key)
      when {
        cmp == 0 -> entry
        cmp < 0 -> {
          result = mid
          left = mid + 1
        }
        else -> right = mid - 1
      }
    }
    return searchInRestartPoint(result, memTableKey)
  }

  private fun searchInRestartPoint(restartIndex: Int, memTableKey: MemTableKey): MemTableEntry? {
    var offset = restartPoints[restartIndex]
    val endOffset = if (restartIndex + 1 < restartPoints.size) {
      restartPoints[restartIndex + 1]
    } else {
      dataSize
    }
    while (offset < endOffset) {
      val entry = getEntryAtOffset(offset)
      val cmp = ByteString.unsignedLexicographicalComparator().compare(entry.key.key, memTableKey.key)
      if (cmp == 0) {
        return entry
      } else if (cmp > 0) {
        return null
      }
      offset += entry.serializedSize + 4 // Move to the next entry
    }
    return null
  }

  private fun getEntryAtOffset(offset: Int): MemTableEntry {
    val buffer = ByteBuffer.wrap(blockData)
    buffer.position(offset)
    val size = buffer.int
    return MemTableEntry.parseFrom(buffer.limit(buffer.position() + size))
  }


}