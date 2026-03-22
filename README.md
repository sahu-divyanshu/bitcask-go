# Bitcask Storage Engine (Go)

A high-performance, append-only key-value storage engine based on the Riak Bitcask paper. This implementation provides $O(1)$ disk reads and write-path isolation during garbage collection.

## Architecture

1.  **Append-Only Log:** All writes (Puts and Deletes) are sequential appends to a binary `.data` file. This maximizes disk throughput by eliminating random I/O seeks.
2.  **In-Memory Hashdir:** The engine maintains a hash map in RAM pointing to the exact `FileID`, `ValueSize`, and `FileOffset` for every live key. Reads execute a single `file.Seek()` followed by a single byte read.
3.  **File Rotation:** To prevent infinite file growth, the active write file is rotated into an immutable read-only state upon hitting a strict byte threshold.
4.  **Non-Blocking Compaction:** A background `Merge()` process iterates over immutable files, compacts live records into a new file, and deletes the bloated files. The active write path is never locked during garbage collection.

## Crash Recovery and Chronological Integrity

File IDs are generated using strictly monotonic Unix nanosecond timestamps. 
During compaction, the merged output file inherits the minimum timestamp of the immutable files it replaces. Upon crash recovery, the engine sorts all files ascending by timestamp. This guarantees that historical compacted data is replayed before fresh active data, preserving strict last-write-wins semantics and preventing data corruption.
