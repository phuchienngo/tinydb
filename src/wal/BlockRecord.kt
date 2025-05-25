package src.wal

import java.nio.ByteBuffer

data class BlockRecord(
  val crc32: Int,
  val blockType: BlockType,
  val data: ByteBuffer
) {
  enum class BlockType(val value: Byte) {
    FULL(1),
    FIRST(2),
    MIDDLE(3),
    LAST(4);

    companion object {
      fun fromValue(value: Byte): BlockType {
        return when (value) {
          FULL.value -> FULL
          FIRST.value -> FIRST
          MIDDLE.value -> MIDDLE
          LAST.value -> LAST
          else -> throw IllegalArgumentException("Unknown block type value: $value")
        }
      }
    }
  }
}
