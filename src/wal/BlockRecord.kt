package src.wal

import java.nio.ByteBuffer

data class BlockRecord(
  val crc32: Int,
  val blockType: BlockType,
  val length: Short,
  val data: ByteBuffer
) {
  companion object {
    fun deserialize(buffer: ByteBuffer): BlockRecord {
      val crc32 = buffer.int
      val blockTypeValue = buffer.get()
      val blockType = when (blockTypeValue) {
        BlockType.FULL.value -> BlockType.FULL
        BlockType.FIRST.value -> BlockType.FIRST
        BlockType.MIDDLE.value -> BlockType.MIDDLE
        BlockType.LAST.value -> BlockType.LAST
        else -> throw IllegalArgumentException("Unknown block type: $blockTypeValue")
      }
      val length = buffer.short
      val dataBytes = ByteArray(length.toInt())
      buffer.get(dataBytes)
      return BlockRecord(crc32, blockType, length, ByteBuffer.wrap(dataBytes))
    }
  }
  enum class BlockType(val value: Byte) {
    FULL(1),
    FIRST(2),
    MIDDLE(3),
    LAST(4)
  }

  fun serialize(buffer: ByteBuffer) {
    buffer.putInt(crc32)
    buffer.put(blockType.value)
    buffer.putShort(length)
    buffer.put(data.duplicate())
  }
}
