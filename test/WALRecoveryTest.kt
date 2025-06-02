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
class WALRecoveryTest {
  private lateinit var testDir: File
  private lateinit var config: Config

  @Before
  fun setUp() {
    testDir = Files.createTempDirectory("leveldb_wal_test_").toFile()
    config = Config(
      dbPath = testDir.toPath(),
      memTableEntriesLimit = 100,
      memTableSizeLimit = 1024 * 1024L,
      ssTableBlockSize = 4096,
      ssTableRestartInterval = 16,
      level0CompactionThreshold = 4
    )
  }

  @After
  fun tearDown() {
    testDir.deleteRecursively()
  }

  // === Basic WAL Recovery Tests ===
  @Test
  fun `data persists after database restart`() {
    // First session: Add data
    val db1 = TinyDB(config)
    db1.put("key1".toByteArray(), "value1".toByteArray())
    db1.put("key2".toByteArray(), "value2".toByteArray())
    db1.put("key3".toByteArray(), "value3".toByteArray())
    db1.close()

    // Second session: Verify data persists (from WAL or SSTables)
    val db2 = TinyDB(config)
    Truth.assertThat(db2.get("key1".toByteArray())).isEqualTo("value1".toByteArray())
    Truth.assertThat(db2.get("key2".toByteArray())).isEqualTo("value2".toByteArray())
    Truth.assertThat(db2.get("key3".toByteArray())).isEqualTo("value3".toByteArray())
    db2.close()
  }

  @Test
  fun `overwrites persist correctly after restart`() {
    // First session: Initial data
    val db1 = TinyDB(config)
    db1.put("key1".toByteArray(), "value1_original".toByteArray())
    db1.put("key2".toByteArray(), "value2".toByteArray())
    db1.close()

    // Second session: Update existing key
    val db2 = TinyDB(config)
    db2.put("key1".toByteArray(), "value1_updated".toByteArray())
    db2.put("key3".toByteArray(), "value3".toByteArray())
    db2.close()

    // Third session: Verify final state
    val db3 = TinyDB(config)
    Truth.assertThat(db3.get("key1".toByteArray())).isEqualTo("value1_updated".toByteArray())
    Truth.assertThat(db3.get("key2".toByteArray())).isEqualTo("value2".toByteArray())
    Truth.assertThat(db3.get("key3".toByteArray())).isEqualTo("value3".toByteArray())
    db3.close()
  }

  @Test
  fun `large amount of data persists after restart`() {
    val testData = mutableMapOf<String, String>()

    // First session: Add large dataset
    val db1 = TinyDB(config)
    for (i in 0 until 1000) {
      val key = "large_key_${String.format("%08d", i)}"
      val value = "large_value_$i${"x".repeat(100)}"
      testData[key] = value
      db1.put(key.toByteArray(), value.toByteArray())
    }
    db1.close()

    // Second session: Verify all data is accessible
    val db2 = TinyDB(config)

    // Check sample of data (checking all 1000 would be slow)
    val sampleIndices = listOf(0, 250, 500, 750, 999)
    sampleIndices.forEach { i ->
      val key = "large_key_${String.format("%08d", i)}"
      val expectedValue = testData[key]!!
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(String(result!!)).isEqualTo(expectedValue)
    }

    db2.close()
  }

  @Test
  fun `multiple restarts preserve data integrity`() {
    val allData = mutableMapOf<String, String>()

    // Session 1: Initial data
    val db1 = TinyDB(config)
    for (i in 0 until 50) {
      val key = "session1_key_$i"
      val value = "session1_value_$i"
      allData[key] = value
      db1.put(key.toByteArray(), value.toByteArray())
    }
    db1.close()

    // Session 2: Add more data and update some existing
    val db2 = TinyDB(config)
    for (i in 0 until 50) {
      val key = "session2_key_$i"
      val value = "session2_value_$i"
      allData[key] = value
      db2.put(key.toByteArray(), value.toByteArray())
    }

    // Update some from session 1
    for (i in 0 until 10) {
      val key = "session1_key_$i"
      val value = "session1_updated_$i"
      allData[key] = value
      db2.put(key.toByteArray(), value.toByteArray())
    }
    db2.close()

    // Session 3: Add more and verify everything
    val db3 = TinyDB(config)
    for (i in 0 until 25) {
      val key = "session3_key_$i"
      val value = "session3_value_$i"
      allData[key] = value
      db3.put(key.toByteArray(), value.toByteArray())
    }

    // Verify all data is correct
    for ((key, expectedValue) in allData) {
      val result = db3.get(key.toByteArray())
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(String(result!!)).isEqualTo(expectedValue)
    }

    db3.close()
  }

  // === Performance Tests ===
  @Test
  fun `restart performance with large dataset`() {
    // Create substantial dataset
    val db1 = TinyDB(config)
    for (i in 0 until 5000) {
      val key = "perf_key_${String.format("%08d", i)}".toByteArray()
      val value = "perf_value_$i${"x".repeat(200)}".toByteArray()
      db1.put(key, value)
    }
    db1.close()

    // Measure restart time (includes WAL recovery if any)
    val startTime = System.currentTimeMillis()
    val db2 = TinyDB(config)
    val startupTime = System.currentTimeMillis() - startTime

    println("Database startup time with 5000 entries: ${startupTime}ms")

    // Verify database is functional after restart
    val testKey = "perf_key_00002500".toByteArray()
    val expectedValue = "perf_value_2500${"x".repeat(200)}".toByteArray()
    Truth.assertThat(db2.get(testKey)).isEqualTo(expectedValue)

    // Should start up reasonably quickly
    Truth.assertThat(startupTime).isLessThan(10000) // Under 10 seconds

    db2.close()
  }

  @Test
  fun `frequent restarts don't lose data`() {
    val persistentData = mutableMapOf<String, String>()
    var totalOperations = 0

    // Simulate multiple short sessions
    for (session in 0 until 20) {
      val db = TinyDB(config)

      // Add a few entries each session
      for (i in 0 until 25) {
        val key = "session${session}_key_$i"
        val value = "session${session}_value_$i"
        persistentData[key] = value
        db.put(key.toByteArray(), value.toByteArray())
        totalOperations++
      }

      // Update a random existing key if we have any
      if (persistentData.isNotEmpty() && session > 0) {
        val randomKey = persistentData.keys.random()
        val newValue = "${randomKey}_updated_in_session_$session"
        persistentData[randomKey] = newValue
        db.put(randomKey.toByteArray(), newValue.toByteArray())
        totalOperations++
      }

      db.close()
    }

    // Final verification
    val finalDb = TinyDB(config)

    println("Total operations across all sessions: $totalOperations")
    println("Final persistent data count: ${persistentData.size}")

    // Verify all final data is correct
    for ( (key, expectedValue) in persistentData) {
      val result = finalDb.get(key.toByteArray())
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(String(result!!)).isEqualTo(expectedValue)
    }

    finalDb.close()
  }

  // === Edge Cases ===
  @Test
  fun `empty database restarts correctly`() {
    // Create empty database
    val db1 = TinyDB(config)
    db1.close()

    // Restart empty database
    val db2 = TinyDB(config)

    // Should work normally
    Truth.assertThat(db2.get("nonexistent".toByteArray())).isNull()

    // Should accept new data
    db2.put("first_key".toByteArray(), "first_value".toByteArray())
    Truth.assertThat(db2.get("first_key".toByteArray())).isEqualTo("first_value".toByteArray())

    db2.close()
  }

  @Test
  fun `binary data survives restart`() {
    // Create binary keys and values
    val binaryKey = byteArrayOf(0x00, 0x01, 0xFF.toByte(), 0x80.toByte(), 0x7F)
    val binaryValue = byteArrayOf(0xFF.toByte(), 0x00, 0xAA.toByte(), 0x55, 0x80.toByte(), 0x7F, 0x01)

    val db1 = TinyDB(config)
    db1.put(binaryKey, binaryValue)
    db1.close()

    val db2 = TinyDB(config)
    val result = db2.get(binaryKey)

    Truth.assertThat(result).isNotNull()
    Truth.assertThat(result).isEqualTo(binaryValue)

    db2.close()
  }

  @Test
  fun `keys with null bytes survive restart`() {
    val keyWithNull = byteArrayOf(1, 0, 2, 3)
    val valueWithNull = byteArrayOf(4, 0, 5, 6)

    val db1 = TinyDB(config)
    db1.put(keyWithNull, valueWithNull)
    db1.close()

    val db2 = TinyDB(config)
    val result = db2.get(keyWithNull)

    Truth.assertThat(result).isEqualTo(valueWithNull)

    db2.close()
  }

  @Test
  fun `very large values survive restart`() {
    val largeKey = "large_value_key".toByteArray()
    val largeValue = ByteArray(1024 * 1024) { (it % 256).toByte() } // 1MB

    val db1 = TinyDB(config)
    db1.put(largeKey, largeValue)
    db1.close()

    val db2 = TinyDB(config)
    val result = db2.get(largeKey)

    Truth.assertThat(result).isNotNull()
    Truth.assertThat(result).isEqualTo(largeValue)

    db2.close()
  }

  // === Compaction Interaction ===
  @Test
  fun `data survives restart with compaction triggers`() {
    val testData = mutableMapOf<String, String>()

    val db1 = TinyDB(config)

    // Add enough data to potentially trigger compaction
    repeat(100) { i ->
      val key = "compaction_key_$i"
      val value = "compaction_value_$i${"x".repeat(1000)}" // Large values
      testData[key] = value
      db1.put(key.toByteArray(), value.toByteArray())
    }

    db1.close()

    // Restart and verify all data is accessible
    val db2 = TinyDB(config)
    for ((key, expectedValue) in testData) {
      val result = db2.get(key.toByteArray())
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(String(result!!)).isEqualTo(expectedValue)
    }

    db2.close()
  }

  @Test
  fun `database functional after restart`() {
    // Session 1: Add initial data
    val db1 = TinyDB(config)
    db1.put("initial_key".toByteArray(), "initial_value".toByteArray())
    db1.close()

    // Session 2: Verify recovery and add more data
    val db2 = TinyDB(config)

    // Verify old data
    Truth.assertThat(db2.get("initial_key".toByteArray())).isEqualTo("initial_value".toByteArray())

    // Database should be fully functional
    db2.put("new_key".toByteArray(), "new_value".toByteArray())
    Truth.assertThat(db2.get("new_key".toByteArray())).isEqualTo("new_value".toByteArray())

    // Update existing
    db2.put("initial_key".toByteArray(), "updated_value".toByteArray())
    Truth.assertThat(db2.get("initial_key".toByteArray())).isEqualTo("updated_value".toByteArray())

    db2.close()
  }
}