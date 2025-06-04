## Overview

TinyDB is a custom key-value database inspired by LSM-tree (Log-Structured Merge Tree) based databases. It provides a simple but efficient storage solution with features including write-ahead logging, sorted string tables (SSTables), and efficient data retrieval mechanisms.

## Features

- **Key-value operations**: Basic put/get/delete operations
- **Persistence**: Data survives across process restarts using write-ahead logging (WAL)
- **Compaction**: Background process to optimize storage and improve read performance
- **Efficient lookups**: Uses bloom filters and index blocks for fast data retrieval
- **Concurrency support**: Thread-safe operations

## Architecture

TinyDB consists of several key components:

- **MemTable**: In-memory data structure that stores recent writes
- **SSTable (Sorted String Table)**: Immutable on-disk storage format for persisting data
- **WAL (Write-Ahead Log)**: Ensures durability by logging operations before applying them
- **Manifest**: Keeps track of database files and their states
- **Compaction**: Merges multiple SSTables to improve read performance and reclaim space

## Project Structure

```
├── src/
│   ├── comparator/      # Comparators for key ordering
│   ├── config/          # Database configuration
│   ├── manifest/        # Database file management
│   ├── memtable/        # In-memory data structure
│   ├── proto/           # Protocol buffer definitions
│   ├── sstable/         # On-disk sorted string tables
│   ├── wal/             # Write-ahead logging
│   ├── lock/            # Database locking mechanism
│   └── TinyDB.kt        # Main database interface
├── test/                # Test cases
├── MODULE.bazel         # Bazel build configuration
└── README.md            # This file
```

## Usage

TinyDB provides a simple API for key-value operations:

```kotlin
// Initialize the database with configuration
val config = Config(
  dbPath = Path.of("/path/to/db"),
  memTableEntriesLimit = 10,
  memTableSizeLimit = 1024 * 1024L,
  ssTableBlockSize = 4096,
  ssTableRestartInterval = 16,
  level0CompactionThreshold = 4
)
val db = TinyDB(config)

// Put a key-value pair
db.put("key".toByteArray(), "value".toByteArray())

// Get a value by key
val value = db.get("key".toByteArray())

// Delete a key
db.delete("key".toByteArray())

// Close the database when done
db.close()
```

## Building and Testing

The project uses Bazel for building and testing:

```bash
# Build the project
bazel build //src:main_app

# Run tests
bazel test //...:all
```

## Technical Details

### Data Storage Format

TinyDB uses an LSM-tree inspired approach:
1. New writes go to the in-memory MemTable
2. When MemTable reaches a size threshold, it's persisted to an immutable SSTable
3. Background compaction merges multiple SSTables to optimize storage

### SSTable Format

Each SSTable consists of:
- Data blocks containing actual key-value pairs
- Index blocks for efficient data lookup
- Bloom filters to quickly determine if a key might exist
- Footer with metadata information

### Consistency and Durability

TinyDB ensures durability through write-ahead logging. Each operation is first recorded in the WAL before being applied to the MemTable, enabling recovery after crashes.