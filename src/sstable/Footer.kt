package src.sstable

import src.proto.sstable.BlockHandle
import java.nio.ByteBuffer

data class Footer(val metaIndexHandle: BlockHandle, val indexHandle: BlockHandle) {
  companion object {
    private const val MAGIC_NUMBER = 0x57fdb4c8L
    private const val FOOTER_SIZE = 40
  }

  fun serialize(): ByteArray {
    val bytes = ByteArray(FOOTER_SIZE)
    val buffer = ByteBuffer.wrap(bytes)

    buffer.putLong(metaIndexHandle.offset)
    buffer.putLong(metaIndexHandle.size)
    buffer.putLong(indexHandle.offset)
    buffer.putLong(indexHandle.size)
    buffer.putLong(MAGIC_NUMBER)
    buffer.flip()

    return bytes
  }
}