@file:Suppress("FunctionName")

package test

import com.google.common.truth.Truth
import org.junit.After
import org.junit.Before
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runners.MethodSorters
import src.TinyDB
import src.config.Config
import java.io.File
import java.nio.file.Files

@FixMethodOrder(MethodSorters.JVM)
class DataBlockIntegrationTest {
  private lateinit var testDir: File
  private lateinit var config: Config
  private lateinit var db: TinyDB

  @Before
  fun setUp() {
    testDir = Files.createTempDirectory("test_db_datablock_test_").toFile()
    config = Config(
      dbPath = testDir.toPath(),
      memTableEntriesLimit = 10, // Small limit to force frequent flushes
      memTableSizeLimit = 1024 * 1024L,
      ssTableBlockSize = 4096,
      ssTableRestartInterval = 16,
      level0CompactionThreshold = 4
    )
    db = TinyDB(config)
  }

  @After
  fun tearDown() {
    try {
      db.close()
    } catch (_: Exception) {
      // ignore close errors due to some test case closed before
    }
    testDir.deleteRecursively()
  }

  // === Basic Data Block Operations Through Public API ===
  @Test
  fun `data blocks store and retrieve single entries correctly`() {
    // Add single entry and force flush to SSTable (data block)
    db.put("test_key".toByteArray(), "test_value".toByteArray())

    // Force flush by exceeding memtable limit
    repeat(12) { i ->
      db.put("flush_key_$i".toByteArray(), "flush_value_$i".toByteArray())
    }

    // Verify original entry can be retrieved (from data block)
    val result = db.get("test_key".toByteArray())
    Truth.assertThat(result).isEqualTo("test_value".toByteArray())
  }

  @Test
  fun `data blocks maintain key ordering`() {
    // Add keys in specific order, then force flush
    val keys = listOf("key_a", "key_b", "key_c", "key_d", "key_e")
    keys.forEach { key ->
      db.put(key.toByteArray(), "value_$key".toByteArray())
    }

    // Force flush to create data blocks
    repeat(10) { i ->
      db.put("flush_$i".toByteArray(), "flush_value".toByteArray())
    }

    // Verify all keys are retrievable (tests data block binary search)
    keys.forEach { key ->
      val result = db.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo("value_$key".toByteArray())
    }

    // Test keys that don't exist (tests data block search boundary conditions)
    Truth.assertThat(db.get("key_0".toByteArray())).isNull() // Before first
    Truth.assertThat(db.get("key_f".toByteArray())).isNull() // After last
    Truth.assertThat(db.get("key_ab".toByteArray())).isNull() // Between existing
  }

  @Test
  fun `data blocks handle many entries with restart points`() {
    // Add enough entries to trigger multiple restart points (16 interval)
    val entryCount = 50
    repeat(entryCount) { i ->
      val key = String.format("block_key_%04d", i)
      val value = "block_value_$i"
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Force flush to create data blocks
    repeat(15) { i ->
      db.put("final_flush_$i".toByteArray(), "final_value".toByteArray())
    }

    // Test retrieval of entries across different restart point regions
    val testIndices = listOf(0, 15, 16, 17, 32, 33, 49) // Cross restart boundaries
    testIndices.forEach { i ->
      val key = String.format("block_key_%04d", i)
      val expectedValue = "block_value_$i"
      val result = db.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }
  }

  // === Binary Search Testing ===
  @Test
  fun `data blocks binary search works with sparse keys`() {
    // Create sparse key space (even numbers only)
    val existingKeys = (0 until 100 step 2).map { String.format("sparse_%04d", it) }
    val missingKeys = (1 until 100 step 2).map { String.format("sparse_%04d", it) }

    // Add existing keys
    existingKeys.forEach { key ->
      db.put(key.toByteArray(), "value_$key".toByteArray())
    }

    // Force flush
    repeat(15) { i ->
      db.put("sparse_flush_$i".toByteArray(), "flush".toByteArray())
    }

    // Test that existing keys are found
    existingKeys.take(10).forEach { key ->
      val result = db.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo("value_$key".toByteArray())
    }

    // Test that missing keys return null (tests binary search miss cases)
    missingKeys.take(10).forEach { key ->
      val result = db.get(key.toByteArray())
      Truth.assertThat(result).isNull()
    }
  }

  @Test
  fun `data blocks handle boundary key searches`() {
    // Add keys with specific patterns to test edge cases
    val keys = listOf(
      "aaa_first",
      "aaa_middle",
      "aaa_last",
      "zzz_first",
      "zzz_middle",
      "zzz_last"
    )

    keys.forEach { key ->
      db.put(key.toByteArray(), "value_$key".toByteArray())
    }

    // Force flush
    repeat(15) { i ->
      db.put("boundary_flush_$i".toByteArray(), "flush".toByteArray())
    }

    // Test boundary conditions
    Truth.assertThat(db.get("aaa_first".toByteArray())).isNotNull() // First key
    Truth.assertThat(db.get("zzz_last".toByteArray())).isNotNull()  // Last key
    Truth.assertThat(db.get("000_before".toByteArray())).isNull()   // Before first
    Truth.assertThat(db.get("zzz_after".toByteArray())).isNull()   // After last
  }

  // === Data Block Size and Limits ===
  @Test
  fun `data blocks respect size limits`() {
    // Create entries that will fill but not exceed block size
    val largeValue = ByteArray(500) { 'X'.code.toByte() } // 500 bytes each

    // Add entries until we likely hit block boundary
    repeat(20) { i ->
      val key = String.format("size_test_%04d", i)
      db.put(key.toByteArray(), largeValue)
    }

    // Force flush to create data blocks
    repeat(15) { i ->
      db.put("size_flush_$i".toByteArray(), "small".toByteArray())
    }

    // Verify all data is retrievable (tests block splitting/handling)
    repeat(20) { i ->
      val key = String.format("size_test_%04d", i)
      val result = db.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(largeValue)
    }
  }

  // === Key Prefix Compression Testing ===
  @Test
  fun `data blocks handle keys with common prefixes`() {
    // Create keys with shared prefixes (tests prefix compression)
    val prefixes = listOf("user_profile_", "user_setting_", "user_history_")
    val suffixes = (0 until 20).map { String.format("%04d", it) }

    prefixes.forEach { prefix ->
      suffixes.forEach { suffix ->
        val key = "$prefix$suffix"
        val value = "value_${prefix}${suffix}"
        db.put(key.toByteArray(), value.toByteArray())
      }
    }

    // Force flush
    repeat(15) { i ->
      db.put("prefix_flush_$i".toByteArray(), "flush".toByteArray())
    }

    // Verify retrieval works correctly
    prefixes.forEach { prefix ->
      suffixes.take(5).forEach { suffix -> // Test sample
        val key = "$prefix$suffix"
        val expectedValue = "value_${prefix}${suffix}"
        val result = db.get(key.toByteArray())
        Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
      }
    }
  }

  // === Data Persistence and Recovery ===
  @Test
  fun `data blocks persist across database restarts`() {
    // Add data and force flush to data blocks
    val testData = mutableMapOf<String, String>()
    repeat(25) { i ->
      val key = "persist_key_$i"
      val value = "persist_value_$i"
      testData[key] = value
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Force flush
    repeat(15) { i ->
      db.put("persist_flush_$i".toByteArray(), "flush".toByteArray())
    }

    db.close()

    // Restart database
    val db2 = TinyDB(config)

    // Verify all data is retrievable from persisted data blocks
    testData.forEach { (key, expectedValue) ->
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }

    db2.close()
  }

  // === Deletion and Data Block Updates ===
  @Test
  fun `deletions work correctly with data blocks`() {
    // Add initial data
    repeat(20) { i ->
      val key = "delete_test_$i"
      val value = "delete_value_$i"
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Force flush to create data blocks
    repeat(15) { i ->
      db.put("delete_flush_$i".toByteArray(), "flush".toByteArray())
    }

    // Delete some entries
    val deletedKeys = listOf(5, 10, 15)
    deletedKeys.forEach { i ->
      db.delete("delete_test_$i".toByteArray())
    }

    // Verify deletions
    deletedKeys.forEach { i ->
      val result = db.get("delete_test_$i".toByteArray())
      Truth.assertThat(result).isNull()
    }

    // Verify non-deleted entries still exist
    val remainingKeys = listOf(0, 1, 7, 12, 19)
    remainingKeys.forEach { i ->
      val result = db.get("delete_test_$i".toByteArray())
      Truth.assertThat(result).isEqualTo("delete_value_$i".toByteArray())
    }
  }

  // === Edge Cases ===
  @Test
  fun `data blocks handle binary keys and values`() {
    // Test binary data in data blocks
    val binaryKeys = listOf(
      byteArrayOf(0x00, 0x01, 0x02),
      byteArrayOf(0x10, 0x20, 0x30),
      byteArrayOf(0xFF.toByte(), 0xFE.toByte(), 0xFD.toByte())
    )

    val binaryValues = listOf(
      byteArrayOf(0xAA.toByte(), 0xBB.toByte(), 0xCC.toByte()),
      byteArrayOf(0x55, 0x66, 0x77),
      byteArrayOf(0x11, 0x22, 0x33)
    )

    // Add binary data
    binaryKeys.zip(binaryValues).forEach { (key, value) ->
      db.put(key, value)
    }

    // Force flush
    repeat(15) { i ->
      db.put("binary_flush_$i".toByteArray(), "flush".toByteArray())
    }

    // Verify binary data retrieval
    binaryKeys.zip(binaryValues).forEach { (key, expectedValue) ->
      val result = db.get(key)
      Truth.assertThat(result).isEqualTo(expectedValue)
    }
  }

  @Test
  fun `data blocks handle null bytes in keys`() {
    val keysWithNulls = listOf(
      byteArrayOf(1, 0, 2),
      byteArrayOf(3, 0, 4),
      byteArrayOf(5, 0, 6)
    )

    keysWithNulls.forEachIndexed { index, key ->
      val value = "value_with_null_$index".toByteArray()
      db.put(key, value)
    }

    // Force flush
    repeat(15) { i ->
      db.put("null_flush_$i".toByteArray(), "flush".toByteArray())
    }

    // Verify retrieval
    keysWithNulls.forEachIndexed { index, key ->
      val expectedValue = "value_with_null_$index".toByteArray()
      val result = db.get(key)
      Truth.assertThat(result).isEqualTo(expectedValue)
    }
  }

  // === Performance Tests ===
  @Test
  fun `data block search performance with large datasets`() {
    // Create large dataset to test data block search performance
    val entryCount = 1000
    repeat(entryCount) { i ->
      val key = String.format("perf_key_%08d", i)
      val value = "perf_value_$i${"x".repeat(100)}"
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Force multiple flushes to create multiple data blocks
    repeat(50) { i ->
      db.put("perf_flush_$i".toByteArray(), "flush".toByteArray())
    }

    // Test search performance on various keys
    val testKeys = listOf(0, 250, 500, 750, 999)

    testKeys.forEach { i ->
      val startTime = System.nanoTime()
      val key = String.format("perf_key_%08d", i)
      val result = db.get(key.toByteArray())
      val searchTime = System.nanoTime() - startTime

      Truth.assertThat(result).isNotNull()
      Truth.assertThat(searchTime).isLessThan(1_000_000) // Less than 1ms
    }
  }

  @Test
  fun `data blocks handle compaction correctly`() {
    // Create enough data to trigger compaction
    repeat(200) { i ->
      val key = "compact_key_$i"
      val value = "compact_value_$i${"x".repeat(500)}"
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Update some keys to create multiple versions
    repeat(50) { i ->
      val key = "compact_key_$i"
      val value = "compact_updated_$i${"y".repeat(500)}"
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Wait a bit for potential background compaction
    Thread.sleep(1000)

    // Verify data integrity after compaction
    repeat(50) { i ->
      val key = "compact_key_$i"
      val expectedValue = "compact_updated_$i${"y".repeat(500)}"
      val result = db.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }

    repeat(50) { i ->
      val index = i + 50
      val key = "compact_key_$index"
      val expectedValue = "compact_value_$index${"x".repeat(500)}"
      val result = db.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }
  }
}