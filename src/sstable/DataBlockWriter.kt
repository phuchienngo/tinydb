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
    val serializedSize = memTableEntry.serializedSize + 4
    if (restartCount++ % restartInterval == 0) {
      restartPoints.add(currentSize)
    }
    randomAccessFile.write(serializedSize)
    randomAccessFile.write(memTableEntry.toByteArray())
    currentSize += serializedSize
    lastKey = memTableEntry.key
  }

  fun finish() {
    for (point in restartPoints) {
      randomAccessFile.write(point)
    }
    randomAccessFile.write(restartPoints.size)
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