package src.sstable

import src.proto.memtable.MemTableEntry
import src.proto.memtable.MemTableKey
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

class DataBlockWriter(
  private val restartInterval: Int,
  private val fileChannel: FileChannel
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
    val buffer = ByteBuffer.allocateDirect(serializedSize)
    buffer.putInt(memTableEntry.serializedSize)
    buffer.put(memTableEntry.toByteArray())
    while (buffer.hasRemaining()) {
      fileChannel.write(buffer)
    }
    fileChannel.force(true)
    currentSize += serializedSize
    lastKey = memTableEntry.key
  }

  fun finish() {
    val bufferSize = 4 + restartPoints.size * 4
    val buffer = ByteBuffer.allocateDirect(bufferSize)
    for (point in restartPoints) {
      buffer.putInt(point)
    }
    buffer.putInt(restartPoints.size)
    buffer.flip()
    while (buffer.hasRemaining()) {
      fileChannel.write(buffer)
    }
    fileChannel.force(true)
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