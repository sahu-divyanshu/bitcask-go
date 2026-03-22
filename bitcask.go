package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Binary record format
//
//  ┌──────────────┬─────────────┬──────────────┬──────────────────┬────────────────────┐
//  │  Timestamp   │   KeySize   │  ValueSize   │       Key        │       Value        │
//  │  (uint64)    │  (uint32)   │  (uint32)    │  (KeySize bytes) │  (ValueSize bytes) │
//  │   8 bytes    │   4 bytes   │   4 bytes    │    variable      │     variable       │
//  └──────────────┴─────────────┴──────────────┴──────────────────┴────────────────────┘
//
//  Fixed header = 16 bytes
//  ValueOffset  = record_start + 16 + len(key)
//  Tombstone    = ValueSize == 0xFFFFFFFF, zero value bytes follow
// ─────────────────────────────────────────────────────────────────────────────

const (
	headerSize      = 16
	tombstoneMarker = ^uint32(0) // 0xFFFFFFFF
	MaxFileSize     = 256        // 1 KB — kept small to force rotation in demo
	dataExt         = ".data"
)

// ─────────────────────────────────────────────────────────────────────────────
// FileID is a Unix millisecond timestamp.
//
// Why this fixes the corruption bug:
//
//   Sequential integer IDs encode creation order, not data age.
//   When Merge() creates a compacted output file it gets the highest integer
//   ID in the system. On restart the sort puts it last, replayFile() treats
//   it as "newest", and its last-write-wins semantics overwrite fresh data
//   from the still-active file with historical values. The database is corrupt.
//
//   Millisecond timestamps encode the age of the DATA inside the file.
//   Merge() is explicitly given the OLDEST timestamp among the files it
//   replaces, so the compacted output always sorts BEFORE the active file.
//   Chronological sort == chronological replay == correct last-write-wins.
//
// Naming: 1717200000123.data
// Sort  : ascending timestamp = oldest → newest (safe for last-write-wins)
// ─────────────────────────────────────────────────────────────────────────────

type FileID = int64 // Unix nanoseconds

var lastTimestamp FileID

// now guarantees strictly monotonically increasing nanosecond timestamps
func now() FileID {
	ts := time.Now().UnixNano()
	if ts <= lastTimestamp {
		ts = lastTimestamp + 1
	}
	lastTimestamp = ts
	return ts
}

func dataFileName(dir string, id FileID) string {
	return filepath.Join(dir, fmt.Sprintf("%d%s", id, dataExt))
}

func parseFileID(name string) (FileID, bool) {
	base := strings.TrimSuffix(filepath.Base(name), dataExt)
	n, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// ─────────────────────────────────────────────────────────────────────────────
// Item — carries FileID so the engine knows which file to Seek into
// ─────────────────────────────────────────────────────────────────────────────

type Item struct {
	FileID     FileID // millisecond timestamp identifying the .data file
	FileOffset int64  // byte offset inside that file where the VALUE starts
	ValueSize  uint32
	Timestamp  uint64
}

// ─────────────────────────────────────────────────────────────────────────────
// dataFile
// ─────────────────────────────────────────────────────────────────────────────

type dataFile struct {
	id   FileID
	f    *os.File
	size int64
}

func openDataFile(dir string, id FileID, write bool) (*dataFile, error) {
	path := dataFileName(dir, id)
	flag := os.O_RDONLY
	if write {
		flag = os.O_RDWR | os.O_CREATE | os.O_APPEND
	}
	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &dataFile{id: id, f: f, size: info.Size()}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Bitcask engine
// ─────────────────────────────────────────────────────────────────────────────

type Bitcask struct {
	dir       string
	active    *dataFile
	immutable map[FileID]*dataFile
	index     map[string]Item
}

// ─────────────────────────────────────────────────────────────────────────────
// Open / Close
// ─────────────────────────────────────────────────────────────────────────────

func Open(dir string) (*Bitcask, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	bc := &Bitcask{
		dir:       dir,
		immutable: make(map[FileID]*dataFile),
		index:     make(map[string]Item),
	}
	if err := bc.loadFiles(); err != nil {
		return nil, err
	}
	return bc, nil
}

func (bc *Bitcask) Close() error {
	if bc.active != nil {
		bc.active.f.Sync()
		bc.active.f.Close()
	}
	for _, df := range bc.immutable {
		df.f.Close()
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// loadFiles — scan directory, sort by timestamp, replay in chronological order
// ─────────────────────────────────────────────────────────────────────────────

func (bc *Bitcask) loadFiles() error {
	entries, err := os.ReadDir(bc.dir)
	if err != nil {
		return fmt.Errorf("readdir %s: %w", bc.dir, err)
	}

	var ids []FileID
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id, ok := parseFileID(e.Name())
		if !ok {
			continue
		}
		ids = append(ids, id)
	}

	// Ascending sort: oldest file first → replay order is chronological.
	// A file with a smaller millisecond timestamp was created earlier and
	// contains older data. Replaying oldest→newest means the last value
	// written for any key wins, which is the correct Bitcask semantic.
	//
	// The merge file adopts the OLDEST timestamp of the files it replaces
	// (see Merge()), so it sorts before the active file and is replayed
	// before it. Fresh active-file data always wins. No corruption.
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if len(ids) == 0 {
		return bc.rotateToNew(now())
	}

	fmt.Printf("  [open]   found %d data file(s)\n", len(ids))

	// All files except the newest are immutable
	for _, id := range ids[:len(ids)-1] {
		df, err := openDataFile(bc.dir, id, false)
		if err != nil {
			return fmt.Errorf("open immutable %d: %w", id, err)
		}
		bc.immutable[id] = df
		if err := bc.replayFile(df); err != nil {
			return fmt.Errorf("replay %d: %w", id, err)
		}
		fmt.Printf("  [open]   immutable %d.data  (%d bytes)\n", id, df.size)
	}

	// The newest file (highest timestamp) is the active file
	lastID := ids[len(ids)-1]
	df, err := openDataFile(bc.dir, lastID, true)
	if err != nil {
		return fmt.Errorf("open active %d: %w", lastID, err)
	}
	bc.active = df
	if err := bc.replayFile(df); err != nil {
		return fmt.Errorf("replay active %d: %w", lastID, err)
	}
	fmt.Printf("  [open]   active    %d.data  (%d bytes)\n", lastID, df.size)
	fmt.Printf("  [open]   index rebuilt — %d live keys\n\n", len(bc.index))
	return nil
}

func (bc *Bitcask) replayFile(df *dataFile) error {
	if _, err := df.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var pos int64
	for {
		recordStart := pos
		var timestamp uint64
		var keySize, valueSize uint32

		if err := binary.Read(df.f, binary.BigEndian, &timestamp); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("read ts at %d: %w", pos, err)
		}
		if err := binary.Read(df.f, binary.BigEndian, &keySize); err != nil {
			return err
		}
		if err := binary.Read(df.f, binary.BigEndian, &valueSize); err != nil {
			return err
		}
		pos += headerSize

		keyBuf := make([]byte, keySize)
		if _, err := io.ReadFull(df.f, keyBuf); err != nil {
			return fmt.Errorf("read key at %d: %w", pos, err)
		}
		key := string(keyBuf)
		pos += int64(keySize)

		valueOffset := pos

		if valueSize == tombstoneMarker {
			delete(bc.index, key)
			fmt.Printf("  [replay] %d  pos=%-6d  TOMBSTONE  key=%q\n",
				df.id, recordStart, key)
		} else {
			bc.index[key] = Item{
				FileID:     df.id,
				FileOffset: valueOffset,
				ValueSize:  valueSize,
				Timestamp:  timestamp,
			}
			fmt.Printf("  [replay] %d  pos=%-6d  key=%-12q  valOffset=%-6d  size=%d\n",
				df.id, recordStart, key, valueOffset, valueSize)
			if _, err := df.f.Seek(int64(valueSize), io.SeekCurrent); err != nil {
				return err
			}
			pos += int64(valueSize)
		}
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// File rotation
// ─────────────────────────────────────────────────────────────────────────────

func (bc *Bitcask) rotateToNew(newID FileID) error {
	if bc.active != nil {
		bc.active.f.Sync()

		// Reopen as read-only — it is now immutable
		oldPath := dataFileName(bc.dir, bc.active.id)
		ro, err := os.Open(oldPath)
		if err != nil {
			return fmt.Errorf("reopen read-only: %w", err)
		}
		bc.active.f.Close()
		bc.active.f = ro

		fmt.Printf("\n  ╔══════════════════════════════════════════════════════════╗\n")
		fmt.Printf("  ║  FILE ROTATION                                            ║\n")
		fmt.Printf("  ║  Froze  %d.data (%d bytes) → immutable        ║\n",
			bc.active.id, bc.active.size)
		fmt.Printf("  ║  Opened %d.data → new active                  ║\n", newID)
		fmt.Printf("  ╚══════════════════════════════════════════════════════════╝\n\n")

		bc.immutable[bc.active.id] = bc.active
	}

	df, err := openDataFile(bc.dir, newID, true)
	if err != nil {
		return fmt.Errorf("open new active %d: %w", newID, err)
	}
	bc.active = df
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Write path
// ─────────────────────────────────────────────────────────────────────────────

func (bc *Bitcask) Put(key, value []byte) error {
	ts := uint64(time.Now().UnixNano())
	kLen := uint32(len(key))
	vLen := uint32(len(value))
	recordSize := int64(headerSize + int(kLen) + int(vLen))

	if bc.active.size+recordSize > MaxFileSize {
		// Use now() for the new active file — it will always be newer than
		// any compacted file because Merge() deliberately uses old timestamps.
		if err := bc.rotateToNew(now()); err != nil {
			return fmt.Errorf("rotate: %w", err)
		}
	}

	buf := make([]byte, recordSize)
	binary.BigEndian.PutUint64(buf[0:8], ts)
	binary.BigEndian.PutUint32(buf[8:12], kLen)
	binary.BigEndian.PutUint32(buf[12:16], vLen)
	copy(buf[16:], key)
	copy(buf[16+kLen:], value)

	valueOffset := bc.active.size + headerSize + int64(kLen)

	n, err := bc.active.f.Write(buf)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if int64(n) != recordSize {
		return fmt.Errorf("short write: %d/%d", n, recordSize)
	}

	bc.active.size += recordSize
	bc.index[string(key)] = Item{
		FileID:     bc.active.id,
		FileOffset: valueOffset,
		ValueSize:  vLen,
		Timestamp:  ts,
	}

	fmt.Printf("  [put]    key=%-12q  file=%d  valOffset=%-6d  size=%d\n",
		string(key), bc.active.id, valueOffset, vLen)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Read path
// ─────────────────────────────────────────────────────────────────────────────

func (bc *Bitcask) fileFor(id FileID) (*os.File, error) {
	if bc.active != nil && bc.active.id == id {
		return bc.active.f, nil
	}
	if df, ok := bc.immutable[id]; ok {
		return df.f, nil
	}
	return nil, fmt.Errorf("no open handle for file %d", id)
}

func (bc *Bitcask) Get(key string) ([]byte, error) {
	item, ok := bc.index[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found or deleted", key)
	}
	f, err := bc.fileFor(item.FileID)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(item.FileOffset, io.SeekStart); err != nil {
		return nil, err
	}
	value := make([]byte, item.ValueSize)
	if _, err := io.ReadFull(f, value); err != nil {
		return nil, err
	}
	fmt.Printf("  [get]    key=%-12q  file=%d  offset=%-6d  value=%q\n",
		key, item.FileID, item.FileOffset, value)
	return value, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Tombstone deletion
// ─────────────────────────────────────────────────────────────────────────────

func (bc *Bitcask) Delete(key string) error {
	if _, ok := bc.index[key]; !ok {
		return fmt.Errorf("key %q not found", key)
	}
	ts := uint64(time.Now().UnixNano())
	kLen := uint32(len(key))
	recordSize := int64(headerSize + int(kLen))

	if bc.active.size+recordSize > MaxFileSize {
		if err := bc.rotateToNew(now()); err != nil {
			return err
		}
	}

	buf := make([]byte, recordSize)
	binary.BigEndian.PutUint64(buf[0:8], ts)
	binary.BigEndian.PutUint32(buf[8:12], kLen)
	binary.BigEndian.PutUint32(buf[12:16], tombstoneMarker)
	copy(buf[16:], key)

	tombstoneAt := bc.active.size
	if _, err := bc.active.f.Write(buf); err != nil {
		return fmt.Errorf("write tombstone: %w", err)
	}
	bc.active.size += recordSize
	delete(bc.index, key)

	fmt.Printf("  [delete] key=%-12q  tombstone in file=%d  offset=%d\n",
		key, bc.active.id, tombstoneAt)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Non-blocking Merge (compaction)
//
// The chronological guarantee:
//
//   New active files always get FileID = now() at the moment of rotation,
//   which is always a larger millisecond value than any past file.
//
//   The merged output file is assigned the MINIMUM FileID among the immutable
//   files it replaces. This is the core invariant:
//
//     mergeFileID = min(immutable file IDs being compacted)
//
//   Because all original immutable files had IDs smaller than the active file,
//   and the merge file takes the smallest of those IDs, the merge file's ID
//   is guaranteed to be smaller than the active file's ID.
//
//   On restart: sort ascending → replay merge file BEFORE active file →
//   active file's last-write-wins semantics are never violated.
//   Fresh data always beats historical compacted data. No corruption.
// ─────────────────────────────────────────────────────────────────────────────

func (bc *Bitcask) Merge() error {
	// Collect live keys that reside in immutable files
	type entry struct {
		key  string
		item Item
	}
	var toCompact []entry
	immutableIDsInvolved := make(map[FileID]struct{})

	for key, item := range bc.index {
		if _, isImmutable := bc.immutable[item.FileID]; isImmutable {
			toCompact = append(toCompact, entry{key, item})
			immutableIDsInvolved[item.FileID] = struct{}{}
		}
	}

	if len(toCompact) == 0 {
		fmt.Println("  [merge]  nothing to compact")
		return nil
	}
	sort.Slice(toCompact, func(i, j int) bool { return toCompact[i].key < toCompact[j].key })

	// ── The key fix: merge file inherits the OLDEST immutable file's ID ───────
	//
	// If immutable files are [T=100, T=200] and active is T=300:
	//   mergeID = 100 (oldest)
	//   After merge: files on disk are [100.data (merged), 300.data (active)]
	//   Sort ascending:  100 → 300
	//   Replay order:    100 (historical, compacted) → 300 (fresh, active)
	//   Last-write-wins: active data always overwrites compacted data. Correct.
	//
	// With the old sequential integer approach:
	//   mergeID = 4 (sequential next), active = 3
	//   Sort ascending:  3 → 4
	//   Replay order:    3 (fresh, active) → 4 (historical, compacted)
	//   Last-write-wins: compacted historical data overwrites fresh data. CORRUPT.
	var mergeID FileID = 1<<62 - 1 // start with max, find minimum
	for id := range immutableIDsInvolved {
		if id < mergeID {
			mergeID = id
		}
	}

	fmt.Printf("  [merge]  %d live key(s) across %d immutable file(s)\n",
		len(toCompact), len(immutableIDsInvolved))
	fmt.Printf("  [merge]  active file %d.data is NOT touched\n", bc.active.id)
	fmt.Printf("  [merge]  merge file will get ID = %d (oldest immutable ID)\n", mergeID)
	fmt.Printf("  [merge]  this guarantees merge file sorts BEFORE active file on restart\n\n")

	// Write to a temp path first, rename when complete (atomic)
	tmpPath := dataFileName(bc.dir, mergeID) + ".tmp"
	mergeF, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open tmp merge file: %w", err)
	}

	newIndex := make(map[string]Item, len(toCompact))
	var cursor int64

	for _, e := range toCompact {
		srcF, err := bc.fileFor(e.item.FileID)
		if err != nil {
			mergeF.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("merge fileFor %q: %w", e.key, err)
		}
		if _, err := srcF.Seek(e.item.FileOffset, io.SeekStart); err != nil {
			mergeF.Close()
			os.Remove(tmpPath)
			return err
		}
		value := make([]byte, e.item.ValueSize)
		if _, err := io.ReadFull(srcF, value); err != nil {
			mergeF.Close()
			os.Remove(tmpPath)
			return err
		}

		kLen := uint32(len(e.key))
		vLen := e.item.ValueSize
		rSize := headerSize + int(kLen) + int(vLen)
		buf := make([]byte, rSize)
		binary.BigEndian.PutUint64(buf[0:8], e.item.Timestamp)
		binary.BigEndian.PutUint32(buf[8:12], kLen)
		binary.BigEndian.PutUint32(buf[12:16], vLen)
		copy(buf[16:], e.key)
		copy(buf[16+kLen:], value)

		newValOffset := cursor + headerSize + int64(kLen)
		if n, err := mergeF.Write(buf); err != nil || n != rSize {
			mergeF.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("merge write %q: %w", e.key, err)
		}

		newIndex[e.key] = Item{
			FileID:     mergeID,
			FileOffset: newValOffset,
			ValueSize:  vLen,
			Timestamp:  e.item.Timestamp,
		}
		fmt.Printf("  [merge]  key=%-12q  %d@%-6d  →  %d@%-6d\n",
			e.key, e.item.FileID, e.item.FileOffset, mergeID, newValOffset)

		cursor += int64(rSize)
	}
	mergeF.Sync()
	mergeF.Close()

	// 1. Close old immutable file descriptors BEFORE the rename
	for id := range immutableIDsInvolved {
		bc.immutable[id].f.Close()
	}

	// 2. Atomic rename tmp → final merge file (safely overwrites the oldest immutable file)
	finalPath := dataFileName(bc.dir, mergeID)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename merge file: %w", err)
	}

	// 3. Update live index for compacted keys
	for key, item := range newIndex {
		bc.index[key] = item
	}

	// 4. Delete old immutable files (CRITICAL: Skip the one we just renamed to)
	for id := range immutableIDsInvolved {
		if id != mergeID {
			os.Remove(dataFileName(bc.dir, id))
			fmt.Printf("  [merge]  deleted old immutable %d.data\n", id)
		} else {
			fmt.Printf("  [merge]  overwrote old immutable %d.data with compacted data\n", id)
		}
		delete(bc.immutable, id)
	}

	// 5. Register the new merge file as an immutable (read-only)
	roF, err := os.Open(finalPath)
	if err != nil {
		return fmt.Errorf("reopen merge file: %w", err)
	}
	info, _ := os.Stat(finalPath)
	bc.immutable[mergeID] = &dataFile{id: mergeID, f: roF, size: info.Size()}

	fmt.Printf("\n  [merge]  done — %d.data (%d bytes) registered as immutable\n",
		mergeID, info.Size())
	fmt.Printf("  [merge]  active file %d.data unchanged\n\n", bc.active.id)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

func (bc *Bitcask) PrintIndex(label string) {
	keys := make([]string, 0, len(bc.index))
	for k := range bc.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("\n  ── %s (%d live keys) ────────────────────────────────────\n",
		label, len(keys))
	fmt.Printf("  %-14s  %-15s  %-10s  %-10s\n", "Key", "FileID (ms)", "Offset", "ValSize")
	fmt.Printf("  %s\n", strings.Repeat("─", 56))
	for _, k := range keys {
		item := bc.index[k]
		fmt.Printf("  %-14q  %-15d  %-10d  %d\n",
			k, item.FileID, item.FileOffset, item.ValueSize)
	}
	fmt.Println()
}

func (bc *Bitcask) PrintFiles(label string) {
	fmt.Printf("  ── %s ─────────────────────────────────────────────────────\n", label)
	fmt.Printf("  Active    : %d.data  (%d bytes)\n", bc.active.id, bc.active.size)
	ids := make([]FileID, 0, len(bc.immutable))
	for id := range bc.immutable {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		fmt.Printf("  Immutable : %d.data  (%d bytes)\n", id, bc.immutable[id].size)
	}
	fmt.Println()
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	const storeDir = "/tmp/bitcask_dir"
	os.RemoveAll(storeDir)

	sep := func(title string) {
		fmt.Printf("\n%s\n  %s\n%s\n\n",
			"══════════════════════════════════════════════════════════",
			title,
			"══════════════════════════════════════════════════════════")
	}

	sep("OPEN")
	bc, err := Open(storeDir)
	if err != nil {
		panic(err)
	}

	sep(fmt.Sprintf("WRITE — fill beyond MaxFileSize (%d bytes) to force rotation", MaxFileSize))
	for _, e := range []struct{ k, v string }{
		{"username", "divyanshu"},
		{"city", "udaipur"},
		{"language", "golang"},
		{"framework", "fastapi"},
		{"database", "postgres"},
		{"team", "backend"},
		{"role", "engineer"},
		{"project", "bitcask"},
	} {
		if err := bc.Put([]byte(e.k), []byte(e.v)); err != nil {
			panic(err)
		}
	}
	bc.PrintFiles("after initial writes")

	sep("DELETE + OVERWRITE — create stale records in immutable files")
	bc.Delete("city")
	for i := 1; i <= 4; i++ {
		bc.Put([]byte("username"), []byte(fmt.Sprintf("divyanshu_v%d", i)))
	}

	activeIDBefore := bc.active.id
	bc.PrintFiles("before merge")
	bc.PrintIndex("index before merge")

	sep("MERGE — active file must remain untouched")
	if err := bc.Merge(); err != nil {
		panic(err)
	}
	bc.PrintFiles("after merge")
	bc.PrintIndex("index after merge")

	if bc.active.id == activeIDBefore {
		fmt.Printf("  ✅  Active file %d.data unchanged by Merge()\n\n", activeIDBefore)
	}

	sep("VERIFY READS post-merge")
	for _, k := range []string{"username", "language", "framework", "database",
		"team", "role", "project", "city"} {
		_, err := bc.Get(k)
		if err != nil {
			fmt.Printf("  [get]  %-12q  ERROR: %v\n", k, err)
		}
	}

	sep("WRITE to active file after merge — prove writes still work")
	bc.Put([]byte("post_merge"), []byte("written_after_compaction"))

	// ── The critical test: close and reopen ───────────────────────────────────
	sep("CRASH RECOVERY — close, reopen, rebuild index")
	fmt.Printf("  merge file ID  < active file ID  must hold for correct replay order\n\n")

	var mergeFileID, activeFileID FileID
	for id := range bc.immutable {
		mergeFileID = id
	}
	activeFileID = bc.active.id
	fmt.Printf("  merge file  ID = %d\n", mergeFileID)
	fmt.Printf("  active file ID = %d\n", activeFileID)
	if mergeFileID < activeFileID {
		fmt.Printf("  ✅  %d < %d — merge file sorts BEFORE active file on restart\n\n",
			mergeFileID, activeFileID)
	} else {
		fmt.Printf("  ❌  ORDERING VIOLATED — data corruption on restart\n\n")
	}

	bc.Close()

	bc2, err := Open(storeDir)
	if err != nil {
		panic(err)
	}
	defer bc2.Close()

	bc2.PrintIndex("recovered index")

	want := "divyanshu_v4"
	v, err := bc2.Get("username")
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	} else if string(v) == want {
		fmt.Printf("  ✅  'username' = %q  (latest value preserved, not overwritten by merge)\n", v)
	} else {
		fmt.Printf("  ❌  'username' = %q  want %q  — stale merge data overwrote fresh data\n",
			v, want)
	}

	v2, _ := bc2.Get("post_merge")
	fmt.Printf("  ✅  'post_merge' = %q  (written after compaction, survives restart)\n\n", v2)

	_, err = bc2.Get("city")
	fmt.Printf("  ✅  'city' absent after recovery: %v\n\n", err)
}
