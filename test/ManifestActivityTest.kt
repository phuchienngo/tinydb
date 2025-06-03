@file:Suppress("FunctionName")

package test

import com.google.common.truth.Truth
import org.junit.After
import org.junit.Before
import org.junit.Test
import src.TinyDB
import src.config.Config
import java.io.File
import java.nio.file.Files

class ManifestActivityTest {
  private lateinit var testDir: File
  private lateinit var config: Config

  @Before
  fun setUp() {
    testDir = Files.createTempDirectory("test_db_manifest_activity_test").toFile()
    config = Config(
      dbPath = testDir.toPath(),
      memTableEntriesLimit = 1,
      memTableSizeLimit = 1024 * 1024L,
      ssTableBlockSize = 4096,
      ssTableRestartInterval = 16,
      level0CompactionThreshold = 4000
    )
  }

  @After
  fun tearDown() {
    testDir.deleteRecursively()
  }

  // === Maximum Manifest Activity Tests ===
  @Test
  fun `manifest logs every SSTable creation with single entry limit`() {
    val db = TinyDB(config)

    // Each put() will create a separate SSTable file and manifest entry
    repeat(50) { i ->
      val key = "manifest_key_${String.format("%04d", i)}"
      val value = "manifest_value_$i"
      db.put(key.toByteArray(), value.toByteArray())
    }

    db.close()

    // Check that many SSTable files were created (one per key)
    val sstableFiles = testDir.listFiles { file ->
      return@listFiles file.extension == "sst"
    }
    Truth.assertThat(sstableFiles?.size ?: 0).isEqualTo(50)

    // Restart and verify manifest correctly tracked all files
    val db2 = TinyDB(config)

    repeat(50) { i ->
      val key = "manifest_key_${String.format("%04d", i)}"
      val expectedValue = "manifest_value_$i"
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }

    db2.close()
  }

  @Test
  fun `manifest tracks file sequence numbers correctly`() {
    val db = TinyDB(config)

    // Create files in specific sequence
    val keys = listOf("aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh")
    keys.forEach { key ->
      db.put(key.toByteArray(), "value_$key".toByteArray())
    }

    db.close()

    // Check file creation
    val sstableFiles = testDir.listFiles { file ->
      return@listFiles file.extension == "sst"
    }
    Truth.assertThat(sstableFiles?.size ?: 0).isEqualTo(8)

    // Restart and verify ordering is maintained
    val db2 = TinyDB(config)

    keys.forEach { key ->
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo("value_$key".toByteArray())
    }

    db2.close()
  }

  @Test
  fun `manifest handles updates creating new files`() {
    val db = TinyDB(config)

    val finalValues = mutableMapOf<String, String>()

    // Initial values - each creates a file
    repeat(20) { i ->
      val key = "update_key_$i"
      val value = "initial_value_$i"
      finalValues[key] = value
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Update each key - creates more files with newer values
    repeat(20) { i ->
      val key = "update_key_$i"
      val value = "updated_value_$i"
      finalValues[key] = value
      db.put(key.toByteArray(), value.toByteArray())
    }

    db.close()

    // Should have 40 SSTable files (20 initial + 20 updates)
    val sstableFiles = testDir.listFiles { file ->
      return@listFiles file.extension == "sst"
    }
    Truth.assertThat(sstableFiles?.size ?: 0).isEqualTo(40)

    // Verify manifest tracked all updates correctly
    val db2 = TinyDB(config)

    finalValues.forEach { (key, expectedValue) ->
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }

    db2.close()
  }

  @Test
  fun `manifest logs deletions as separate files`() {
    val db = TinyDB(config)

    // Create initial data
    repeat(15) { i ->
      val key = "delete_test_$i"
      val value = "delete_value_$i"
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Delete every other key (creates tombstone files)
    val deletedKeys = mutableListOf<String>()
    repeat(15) { i ->
      if (i % 2 == 0) {
        val key = "delete_test_$i"
        deletedKeys.add(key)
        db.delete(key.toByteArray())
      }
    }

    db.close()

    // Should have 15 + 8 = 23 files (initial + tombstone files)
    val sstableFiles = testDir.listFiles { file ->
      return@listFiles file.extension == "sst"
    }
    Truth.assertThat(sstableFiles?.size ?: 0).isEqualTo(23)

    // Verify manifest correctly handles deletions
    val db2 = TinyDB(config)

    // Deleted keys should be gone
    deletedKeys.forEach { key ->
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isNull()
    }

    // Remaining keys should exist
    repeat(15) { i ->
      if (i % 2 != 0) {
        val key = "delete_test_$i"
        val expectedValue = "delete_value_$i"
        val result = db2.get(key.toByteArray())
        Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
      }
    }

    db2.close()
  }

  @Test
  fun `manifest recovery with many SSTable files`() {
    val allData = mutableMapOf<String, String>()

    // Session 1: Create many files
    val db1 = TinyDB(config)

    repeat(50) { i ->
      val key = "session1_$i"
      val value = "session1_value_$i"
      allData[key] = value
      db1.put(key.toByteArray(), value.toByteArray())
    }
    db1.close()

    // Session 2: Add more files
    val db2 = TinyDB(config)

    repeat(50) { i ->
      val key = "session2_$i"
      val value = "session2_value_$i"
      allData[key] = value
      db2.put(key.toByteArray(), value.toByteArray())
    }
    db2.close()

    // Session 3: Final verification
    val db3 = TinyDB(config)

    println("Total expected files: 100")
    val sstableFiles = testDir.listFiles { file ->
      return@listFiles file.extension == "sst"
    }
    println("Actual SSTable files: ${sstableFiles?.size ?: 0}")

    allData.forEach { (key, expectedValue) ->
      val result = db3.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }

    db3.close()
  }

  @Test
  fun `manifest performance with high file creation rate`() {
    val startTime = System.currentTimeMillis()

    val db = TinyDB(config)

    // Rapidly create many SSTable files
    repeat(200) { i ->
      val key = "rapid_file_${String.format("%08d", i)}"
      val value = "rapid_value_$i"
      db.put(key.toByteArray(), value.toByteArray())

      if (i % 50 == 0) {
        val elapsed = System.currentTimeMillis() - startTime
        println("Created $i files in ${elapsed}ms")
      }
    }

    val totalTime = System.currentTimeMillis() - startTime
    println("Created 200 SSTable files in ${totalTime}ms")

    db.close()

    // Restart performance test
    val restartStart = System.currentTimeMillis()
    val db2 = TinyDB(config)
    val restartTime = System.currentTimeMillis() - restartStart

    println("Restart with 200 files took ${restartTime}ms")

    // Verify functionality
    val testKey = "rapid_file_00000100"
    val result = db2.get(testKey.toByteArray())
    Truth.assertThat(result).isEqualTo("rapid_value_100".toByteArray())

    Truth.assertThat(restartTime).isLessThan(5000) // Should restart within 5 seconds

    db2.close()
  }

  @Test
  fun `manifest handles interleaved operations creating many files`() {
    val db = TinyDB(config)

    val finalState = mutableMapOf<String, String>()

    // Interleave puts, updates, and deletes
    repeat(30) { i ->
      // Put new key
      val newKey = "new_key_$i"
      val newValue = "new_value_$i"
      finalState[newKey] = newValue
      db.put(newKey.toByteArray(), newValue.toByteArray())

      // Update existing key if available
      if (i > 5) {
        val updateKey = "new_key_${i - 5}"
        val updateValue = "updated_value_${i - 5}"
        finalState[updateKey] = updateValue
        db.put(updateKey.toByteArray(), updateValue.toByteArray())
      }

      // Delete old key if available
      if (i > 10) {
        val deleteKey = "new_key_${i - 10}"
        finalState.remove(deleteKey)
        db.delete(deleteKey.toByteArray())
      }
    }

    db.close()

    // Should have created many files from all operations
    val sstableFiles = testDir.listFiles { file ->
      return@listFiles file.extension == "sst"
    }
    println("Files created from interleaved operations: ${sstableFiles?.size ?: 0}")
    Truth.assertThat(sstableFiles?.size ?: 0).isGreaterThan(60) // 30 puts + 25 updates + 20 deletes

    // Verify final state
    val db2 = TinyDB(config)

    finalState.forEach { (key, expectedValue) ->
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }

    // Verify deleted keys are gone
    repeat(20) { i ->
      val deletedKey = "new_key_$i"
      if (!finalState.containsKey(deletedKey)) {
        val result = db2.get(deletedKey.toByteArray())
        Truth.assertThat(result).isNull()
      }
    }

    db2.close()
  }

  @Test
  fun `manifest file size grows with SSTable count`() {
    val db = TinyDB(config)

    val manifestSizes = mutableListOf<Long>()

    // Track manifest size growth
    repeat(10) { batch ->
      repeat(20) { i ->
        val key = "growth_test_${batch * 20 + i}"
        val value = "growth_value_${batch * 20 + i}"
        db.put(key.toByteArray(), value.toByteArray())
      }

      // Check manifest size after each batch
      val manifestFiles = testDir.listFiles { file ->
        return@listFiles file.extension == "mnf"
      }
      val manifestSize = manifestFiles?.get(0)?.length() ?: 0
      manifestSizes.add(manifestSize)

      println("After ${(batch + 1) * 20} files, manifest size: $manifestSize bytes")
    }

    db.close()

    // Manifest should grow with number of files
    Truth.assertThat(manifestSizes.last()).isGreaterThan(manifestSizes.first())

    // Verify all data is accessible
    val db2 = TinyDB(config)

    repeat(200) { i ->
      val key = "growth_test_$i"
      val expectedValue = "growth_value_$i"
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isEqualTo(expectedValue.toByteArray())
    }

    db2.close()
  }

  @Test
  fun `manifest handles binary keys with single entry files`() {
    val db = TinyDB(config)

    val binaryKeys = mutableListOf<ByteArray>()
    val expectedValues = mutableListOf<ByteArray>()

    // Create binary keys that challenge manifest encoding
    repeat(25) { i ->
      val key = byteArrayOf(
        (i % 256).toByte(),
        ((i * 2) % 256).toByte(),
        ((i * 3) % 256).toByte(),
        0, // null byte
        (255 - i % 256).toByte()
      )
      val value = byteArrayOf(
        ((i * 7) % 256).toByte(),
        ((i * 11) % 256).toByte(),
        0, // null byte
        ((i * 13) % 256).toByte()
      )

      binaryKeys.add(key)
      expectedValues.add(value)
      db.put(key, value)
    }

    db.close()

    // Verify manifest correctly handled binary keys
    val db2 = TinyDB(config)

    binaryKeys.zip(expectedValues).forEach { (key, expectedValue) ->
      val result = db2.get(key)
      Truth.assertThat(result).isEqualTo(expectedValue)
    }

    db2.close()
  }
}