package src.sstable

import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import java.io.RandomAccessFile

class DataBlockWriter(
  private val restartInterval: Int,
  private val randomAccessFile: RandomAccessFile
) {
  private var currentSize = 0
  private var restartCount = 0
  private var restartPoints = mutableSetOf<Int>()
  private var lastKey = MemTableKey.getDefaultInstance()

  fun write(memTableEntry: MemTableEntry) {
    if (restartCount++ % restartInterval == 0) {
      restartPoints.add(currentSize)
    }
    randomAccessFile.writeInt(memTableEntry.serializedSize)
    randomAccessFile.write(memTableEntry.toByteArray())
    currentSize += memTableEntry.serializedSize + 4
    lastKey = memTableEntry.key
  }

  fun finish() {
    for (point in restartPoints) {
      randomAccessFile.writeInt(point)
      currentSize += 4
    }
    randomAccessFile.writeInt(restartPoints.size)
    currentSize += 4
    randomAccessFile.fd.sync()
  }

  fun lastKey(): MemTableKey {
    return lastKey
  }

  fun writtenSize(): Int {
    return currentSize
  }

  fun reset() {
    currentSize = 0
    restartCount = 0
    restartPoints.clear()
    lastKey = MemTableKey.getDefaultInstance()
  }
}