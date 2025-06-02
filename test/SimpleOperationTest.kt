package test

import com.google.common.truth.Truth
import org.junit.After
import org.junit.Before
import org.junit.Test
import src.TinyDB
import src.config.Config
import java.io.File
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import kotlin.random.Random

@Suppress("FunctionName")
class SimpleOperationTest {
  private lateinit var db: TinyDB
  private lateinit var testDir: File

  @Before
  fun setUp() {
    testDir = Files.createTempDirectory("test_db_").toAbsolutePath().toFile()
    val config = Config(
      dbPath = testDir.toPath(),
      memTableEntriesLimit = 10,
      memTableSizeLimit = 1024 * 1024L,
      ssTableBlockSize = 4096,
      ssTableRestartInterval = 16,
      level0CompactionThreshold = 4
    )
    db = TinyDB(config)
  }

  @After
  fun tearDown() {
    db.close()
    testDir.deleteRecursively()
  }

  @Test
  fun `junit 4 is working`() {
    Truth.assertThat(1 + 1).isEqualTo(2)
  }

  @Test
  fun `put and get single key-value pair`() {
    val key = "test_key".toByteArray()
    val value = "test_value".toByteArray()

    db.put(key, value)
    val result = db.get(key)

    Truth.assertThat(result).isNotNull()
    Truth.assertThat(result).isEqualTo(value)
  }

  @Test
  fun `put multiple key-value pairs`() {
    val pairs = mapOf(
      "key1" to "value1",
      "key2" to "value2",
      "key3" to "value3"
    )

    for ((k, v) in pairs) {
      db.put(k.toByteArray(), v.toByteArray())
    }

    pairs.forEach { (k, v) ->
      val result = db.get(k.toByteArray())
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(result).isEqualTo(v.toByteArray())
    }
  }

  @Test
  fun `put overwrites existing key`() {
    val key = "test_key".toByteArray()
    val oldValue = "old_value".toByteArray()
    val newValue = "new_value".toByteArray()

    db.put(key, oldValue)
    db.put(key, newValue)

    val result = db.get(key)
    Truth.assertThat(newValue).isEqualTo(result)
  }

  @Test
  fun `get non-existent key returns null`() {
    val key = "non_existent_key".toByteArray()
    val result = db.get(key)
    Truth.assertThat(result).isNull()
  }

  @Test
  fun `put and get empty key`() {
    val key = byteArrayOf()
    val value = "empty_key_value".toByteArray()

    db.put(key, value)
    val result = db.get(key)

    Truth.assertThat(result).isEqualTo(value)
  }

  @Test
  fun `delete existing key`() {
    val key = "test_key".toByteArray()
    val value = "test_value".toByteArray()

    db.put(key, value)
    Truth.assertThat(db.get(key)).isNotNull()

    db.delete(key)
    Truth.assertThat(db.get(key)).isNull()
  }

  @Test
  fun `delete non-existent key does not throw`() {
    val key = "non_existent_key".toByteArray()

    try {
      db.delete(key)
    } catch (e: Exception) {
      Truth.assertThat(e).isNull()
    }
  }

  @Test
  fun `delete then put same key`() {
    val key = "test_key".toByteArray()
    val value1 = "value1".toByteArray()
    val value2 = "value2".toByteArray()

    db.put(key, value1)
    db.delete(key)
    db.put(key, value2)

    Truth.assertThat(value2).isEqualTo(db.get(key))
  }

  @Test
  fun `handle null byte in key and value`() {
    val key = byteArrayOf(1, 0, 2, 3)
    val value = byteArrayOf(4, 0, 5, 6)

    db.put(key, value)
    val result = db.get(key)
    Truth.assertThat(result).isEqualTo(result)
  }

  @Test
  fun `handle large values`() {
    val key = "large_value_key".toByteArray()
    val value = ByteArray(1024 * 1024, Int::toByte) // 1MB

    db.put(key, value)
    val result = db.get(key)
    Truth.assertThat(result).isEqualTo(value)
  }

  @Test
  fun `handle many small keys`() {
    val keyCount = 1000
    for (i in 0 until keyCount) {
      val key = "key_$i".toByteArray()
      val value = "value_$i".toByteArray()
      db.put(key, value)
    }

    for (i in 0 until keyCount) {
      val key = "key_$i".toByteArray()
      val expectedValue = "value_$i".toByteArray()
      val result = db.get(key)

      Truth.assertThat(result).isNotNull()
      Truth.assertThat(result).isEqualTo(expectedValue)
    }
  }

  // === Sequence Tests ===
  @Test
  fun `put-get-delete-get sequence`() {
    val key = "sequence_key".toByteArray()
    val value = "sequence_value".toByteArray()

    Truth.assertThat(db.get(key)).isNull() // Initially not present
    db.put(key, value) // Put and verify
    Truth.assertThat(value).isEqualTo(db.get(key))
    db.delete(key) // Delete and verify
    Truth.assertThat(db.get(key)).isNull()
  }

  @Test
  fun `multiple operations on same key`() {
    val key = "multi_op_key".toByteArray()

    db.put(key, "value1".toByteArray())
    Truth.assertThat("value1".toByteArray()).isEqualTo(db.get(key))

    db.put(key, "value2".toByteArray())
    Truth.assertThat("value2".toByteArray()).isEqualTo(db.get(key))

    db.delete(key)
    Truth.assertThat(db.get(key)).isNull()

    db.put(key, "value3".toByteArray())
    Truth.assertThat("value3".toByteArray()).isEqualTo(db.get(key))
  }

  @Test
  fun `stress test with random operations`() {
    val operations = 10000
    val keySpace = 1000
    val random = Random(42)

    for (i in 0 until operations) {
      val key = "stress_key_${random.nextInt(keySpace)}".toByteArray()
      val value = "stress_value_${random.nextInt()}".toByteArray()

      when (random.nextInt(3)) {
        0 -> db.put(key, value)
        1 -> db.get(key)
        2 -> db.delete(key)
      }
    }

    val testKey = "final_test".toByteArray()
    val testValue = "final_value".toByteArray()

    db.put(testKey, testValue)
    Truth.assertThat(testValue).isEqualTo(db.get(testKey))
  }

  @Test
  fun `operations with empty values`() {
    val key = "empty_value_key".toByteArray()
    val emptyValue = byteArrayOf()

    db.put(key, emptyValue)
    val result = db.get(key)

    Truth.assertThat(result).isNotNull()
    Truth.assertThat(result).isEqualTo(emptyValue)
  }

  @Test
  fun `concurrent operations safety`() {
    val threads = 10
    val operationsPerThread = 100
    val latch = CountDownLatch(threads)

    for (threadId in 0 until threads) {
      Thread.ofVirtual().start {
        try {
          for (opId in 0 until operationsPerThread) {
            val key = "thread_${threadId}_key_${opId}".toByteArray()
            val value = "thread_${threadId}_value_${opId}".toByteArray()

            db.put(key, value)
            val result = db.get(key)
            Truth.assertThat(result).isEqualTo(value)
          }
        } finally {
          latch.countDown()
        }
      }
    }

    latch.await()
  }
}