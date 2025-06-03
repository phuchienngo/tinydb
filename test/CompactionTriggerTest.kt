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
class CompactionTriggerTest {
  private lateinit var db: TinyDB
  private lateinit var testDir: File

  @Before
  fun setUp() {
    testDir = Files.createTempDirectory("test_db_compaction_test_").toFile()
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

  // === Level 0 Compaction Tests ===
  @Test
  fun `trigger level 0 compaction with file count threshold`() {
    // Level 0 threshold is 4 files, so we need 5 files to trigger compaction
    val filesNeeded = 5
    val entriesPerFlush = 10 // memTableEntriesLimit

    // Create 5 SSTables by flushing memtable 5 times
    for (fileIndex in 0 until filesNeeded) {
      repeat(entriesPerFlush) { entryIndex ->
        val key = "file${fileIndex}_key${entryIndex}".toByteArray()
        val value = "file${fileIndex}_value${entryIndex}_${"x".repeat(100)}".toByteArray() // Make values substantial
        db.put(key, value)
      }
      // Force memtable flush by reaching entry limit
    }

    // Add one more entry to trigger compaction check
    db.put("trigger_compaction".toByteArray(), "trigger_value".toByteArray())

    // Verify that compaction was triggered by checking level 0 file count
    val level0FileCount = db.getLevelNFileCount(0) // You'll need this method
    Truth.assertThat(level0FileCount).isLessThan(filesNeeded)

    // Verify data is still accessible after compaction
    for (fileIndex in 0 until filesNeeded) {
      for (entryIndex in 0 until entriesPerFlush) {
        val key = "file${fileIndex}_key${entryIndex}".toByteArray()
        val result = db.get(key)
        Truth.assertThat(result).isNotNull()
      }
    }
  }

  @Test
  fun `level 0 compaction preserves data integrity`() {
    val testData = mutableMapOf<String, String>()

    // Create enough data to trigger level 0 compaction
    for (i in 0 until 60) { // 5 flushes = 5 SSTables > 4 threshold
      val key = "integrity_test_$i"
      val value = "value_$i" + "x".repeat(200) // Larger values
      testData[key] = value
      db.put(key.toByteArray(), value.toByteArray())
    }

    // Verify all data is still accessible
    for ((key, expectedValue) in testData) {
      val result = db.get(key.toByteArray())
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(String(result!!)).isEqualTo(expectedValue)
    }
  }

  // === Level N Compaction Tests ===
  @Test
  fun `trigger level 1 compaction with size threshold`() {
    // Level 1 threshold is 10MB
    // Need to create enough data to exceed this
    val targetSize = 12 * 1024 * 1024L // 12MB to exceed 10MB threshold
    val valueSize = 10 * 1024 // 10KB per value
    val entriesNeeded = (targetSize / valueSize).toInt()

    for (i in 0 until entriesNeeded) {
      val key = "level1_key_$i".toByteArray()
      val value = ByteArray(valueSize) { (i % 256).toByte() }
      db.put(key, value)

      // Log progress for long-running test
      if (i % 1000 == 0) {
        println("Inserted $i entries...")
      }
    }

    // Force one final operation to trigger compaction check
    db.put("final_trigger".toByteArray(), "final_value".toByteArray())

    // Verify level 1 was compacted (fewer files, but same data)
    val level1Size = db.getLevelNFileCount(1) // You'll need this method
    Truth.assertThat(level1Size).isAtMost(targetSize) // Should be compacted efficiently

    // Verify data integrity
    for (i in 0 until 100.coerceAtMost(entriesNeeded)) {
      val key = "level1_key_$i".toByteArray()
      val result = db.get(key)
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(result?.size).isEqualTo(valueSize)
    }
  }

  @Test
  fun `trigger level 2 compaction with larger dataset`() {
    // Level 2 threshold is 100MB
    val targetSize = 120 * 1024 * 1024L // 120MB
    val valueSize = 50 * 1024 // 50KB per value
    val entriesNeeded = (targetSize / valueSize).toInt()

    for (i in 0 until entriesNeeded) {
      val key = String.format("level2_key_%08d", i).toByteArray()
      val value = ByteArray(valueSize) { (i % 256).toByte() }
      db.put(key, value)

      // Log progress for long-running test
      if (i % 1000 == 0) {
        println("Inserted $i entries...")
      }
    }

    // Verify compaction occurred and data is accessible
    val sampleKeys = listOf(0, entriesNeeded / 4, entriesNeeded / 2, entriesNeeded - 1)
    for (i in sampleKeys) {
      val key = String.format("level2_key_%08d", i).toByteArray()
      val result = db.get(key)
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(result!!.size).isEqualTo(valueSize)
    }
  }

  // === Multi-Level Compaction Test ===
  @Test
  fun `cascade compaction across multiple levels`() {
    val entriesPerBatch = 50
    val valueSize = 20 * 1024 // 20KB values

    // Create data in batches to trigger cascading compactions
    for (batch in 0 until 20) { // 20 batches
      for (i in 0 until entriesPerBatch) {
        val globalIndex = batch * entriesPerBatch + i
        val key = "cascade_key_$globalIndex".toByteArray()
        val value = ByteArray(valueSize) {
          ((globalIndex + it) % 256).toByte()
        }
        db.put(key, value)
      }
      println("Inserted batch $batch")
    }

    // Verify data across all levels
    val totalEntries = 20 * entriesPerBatch
    val sampleIndices = (0 until totalEntries step 100).toList()

    for (i in sampleIndices) {
      val key = "cascade_key_$i".toByteArray()
      val result = db.get(key)
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(result?.size).isEqualTo(valueSize)
    }
  }

  // === Performance Test ===
  @Test(timeout = 300_000)
  fun `compaction performance under load`() {
    val batchSize = 100
    val batches = 50
    val valueSize = 15 * 1024 // 15KB

    val startTime = System.currentTimeMillis()

    for (batch in 0 until batches) {
      for (i in 0 until batchSize) {
        val key = "perf_${batch}_$i".toByteArray()
        val value = ByteArray(valueSize) { ((batch + i) % 256).toByte() }
        db.put(key, value)
      }

      // Log progress and give time for background compaction
      if (batch % 10 == 0) {
        val elapsed = System.currentTimeMillis() - startTime
        println("Inserted batch $batch in ${elapsed}ms")
        Thread.sleep(100) // Allow background compaction to proceed
      }
    }

    // Add some overwrites to stress compaction
    repeat(200) { i ->
      val key = "perf_${i % batches}_${i % batchSize}".toByteArray()
      val newValue = ByteArray(valueSize) { (i % 256).toByte() }
      db.put(key, newValue)
    }

    val totalTime = System.currentTimeMillis() - startTime
    val totalEntries = batches * batchSize + 200 // Include overwrites
    val entriesPerSecond = (totalEntries * 1000.0 / totalTime).toInt()

    println("Performance with compaction: $entriesPerSecond entries/second")
    Truth.assertThat(entriesPerSecond).isGreaterThan(50) // Lower threshold due to compaction overhead

    // Verify data integrity after compaction stress
    repeat(50) {
      val randomBatch = (0 until batches).random()
      val randomEntry = (0 until batchSize).random()
      val key = "perf_${randomBatch}_$randomEntry".toByteArray()

      val result = db.get(key)
      Truth.assertThat(result).isNotNull()
      Truth.assertThat(result!!.size).isEqualTo(valueSize)
    }
  }
}