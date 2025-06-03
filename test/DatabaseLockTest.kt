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
class DatabaseLockTest {

  private lateinit var testDir: File
  private lateinit var config: Config

  @Before
  fun setUp() {
    testDir = Files.createTempDirectory("test_db_lock_test").toFile()
    config = Config(
      dbPath = testDir.toPath(),
      memTableEntriesLimit = 10,
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

  // === Basic Lock Functionality ===
  @Test
  fun `single database instance opens successfully`() {
    // Should be able to open database normally
    val db = TinyDB(config)

    // Verify it's functional
    db.put("test_key".toByteArray(), "test_value".toByteArray())
    val result = db.get("test_key".toByteArray())
    Truth.assertThat(result).isEqualTo("test_value".toByteArray())

    db.close()
  }

  @Test
  fun `cannot open database twice simultaneously`() {
    // Open first instance
    val db1 = TinyDB(config)

    // Attempt to open second instance should fail
    try {
      TinyDB(config)
      Truth.assertThat(true).isFalse()
    } catch (e: Exception) {
      Truth.assertThat(e).isInstanceOf(IllegalStateException::class.java)
    }

    db1.close()
  }

  @Test
  fun `can reopen database after closing first instance`() {
    // Open and close first instance
    val db1 = TinyDB(config)
    db1.put("key1".toByteArray(), "value1".toByteArray())
    db1.close()

    // Should be able to open second instance after first is closed
    val db2 = TinyDB(config)

    // Verify data persisted and new instance works
    val result = db2.get("key1".toByteArray())
    Truth.assertThat(result).isEqualTo("value1".toByteArray())

    db2.put("key2".toByteArray(), "value2".toByteArray())
    Truth.assertThat(db2.get("key2".toByteArray())).isEqualTo("value2".toByteArray())

    db2.close()
  }

  // === Lock File Management ===
  @Test
  fun `lock file is created when database opens`() {
    val lockFile = File(testDir, "LOCK")

    // Lock file should not exist initially
    Truth.assertThat(lockFile.exists()).isFalse()

    val db = TinyDB(config)

    // Lock file should exist when database is open
    Truth.assertThat(lockFile.exists()).isTrue()

    db.close()
  }
}