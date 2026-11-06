package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Public errors
// ---------------------------------------------------------------------------

var (
	ErrKeyTooLarge   = errors.New("kvdb: key exceeds 64KiB")
	ErrKeyEmpty      = errors.New("kvdb: key is empty")
	ErrValueTooLarge = errors.New("kvdb: value exceeds 64MiB")
	ErrNotFound      = errors.New("kvdb: key not found")
	ErrCorrupt       = errors.New("kvdb: data corruption detected")
	ErrClosed        = errors.New("kvdb: engine is closed")
	ErrReadOnly      = errors.New("kvdb: engine is read-only")
	ErrBroken        = errors.New("kvdb: engine is broken (fsync failed)")
	ErrBatchTooLarge = errors.New("kvdb: batch exceeds 100000 operations")
	ErrReservedKey   = errors.New("kvdb: key is reserved for internal use")
	ErrBatchEmpty    = errors.New("kvdb: batch is empty")
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	headerSize   = 16 // crc(4) + timestamp(4) + keySize(4) + valSize(4)
	maxKeySize   = 64 * 1024
	maxValueSize = 64 * 1024 * 1024
	tombstoneVal = 0xFFFFFFFF
	maxBatchOps  = 100000
)

var batchMarkerKey = []byte("\x00\x00kvdb-batch-v1")

// ---------------------------------------------------------------------------
// Index entry
// ---------------------------------------------------------------------------

type indexEntry struct {
	fileID    int
	valueSize int32
	valuePos  int64
	timestamp uint32
}

// ---------------------------------------------------------------------------
// Batch operation
// ---------------------------------------------------------------------------

// BatchOp represents a single operation inside a Batch.
type BatchOp struct {
	Key   []byte
	Value []byte // nil means Delete
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// Stats holds engine statistics.
type Stats struct {
	Keys     int
	DataSize int64
	FilePath string
	ReadOnly bool
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// Options controls how the engine is opened.
type Options struct {
	ReadOnly bool
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

// Engine is a Bitcask-style key-value store.
type Engine struct {
	mu          sync.RWMutex
	path        string
	file        *os.File
	index       map[string]indexEntry
	offset      int64
	readOnly    bool
	closed      bool
	broken      bool
	hasTornTail bool
}

// Open opens or creates a kvdb database at path.
func Open(path string) (*Engine, error) {
	return OpenWithOptions(path, Options{})
}

// OpenWithOptions opens or creates a kvdb database with the given options.
func OpenWithOptions(path string, opts Options) (*Engine, error) {
	compactPath := path + ".compact"

	if !opts.ReadOnly {
		// Remove leftover compact file on read-write open.
		_ = os.Remove(compactPath)
	}

	var file *os.File
	var err error
	created := false

	if opts.ReadOnly {
		file, err = os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			return nil, fmt.Errorf("kvdb: open: %w", err)
		}
	} else {
		file, err = os.OpenFile(path, os.O_RDWR, 0644)
		if errors.Is(err, os.ErrNotExist) {
			file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
			if err != nil {
				return nil, fmt.Errorf("kvdb: create: %w", err)
			}
			created = true
			// Fsync the parent directory when creating the file.
			if err2 := syncDir(filepath.Dir(path)); err2 != nil {
				file.Close()
				return nil, fmt.Errorf("kvdb: sync dir: %w", err2)
			}
		} else if err != nil {
			return nil, fmt.Errorf("kvdb: open: %w", err)
		}
	}

	e := &Engine{
		path:     path,
		file:     file,
		index:    make(map[string]indexEntry),
		readOnly: opts.ReadOnly,
	}

	if !created {
		truncateAt, err := e.recover()
		if err != nil {
			file.Close()
			return nil, err
		}
		if truncateAt >= 0 {
			e.hasTornTail = true
			if opts.ReadOnly {
				// Report the problem but never modify the file.
				_, _ = fmt.Fprintf(os.Stderr,
					"kvdb: read-only open: torn/corrupt tail detected at offset %d\n",
					truncateAt)
			} else {
				if err := file.Truncate(truncateAt); err != nil {
					file.Close()
					return nil, fmt.Errorf("kvdb: truncate: %w", err)
				}
				if err := file.Sync(); err != nil {
					file.Close()
					return nil, fmt.Errorf("kvdb: sync after truncate: %w", err)
				}
				e.offset = truncateAt
			}
		}
	}

	return e, nil
}

// Close closes the engine.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.file.Close()
}

// ---------------------------------------------------------------------------
// Put / Get / Delete
// ---------------------------------------------------------------------------

// Put stores a key-value pair. Returns nil only after file.Sync() succeeds.
func (e *Engine) Put(key, value []byte) error {
	if err := validateUserKey(key); err != nil {
		return err
	}
	if len(value) > maxValueSize {
		return ErrValueTooLarge
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.putLocked(key, value, nowTimestamp())
}

// Get retrieves the value for key. Returns ErrNotFound if absent.
func (e *Engine) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, ErrClosed
	}

	ent, ok := e.index[string(key)]
	if !ok {
		return nil, ErrNotFound
	}

	// Re-read the whole record and verify CRC.
	recStart := ent.valuePos - int64(len(key)) - int64(headerSize)
	recSize := int64(headerSize) + int64(len(key)) + int64(ent.valueSize)
	buf := make([]byte, recSize)
	if _, err := e.file.ReadAt(buf, recStart); err != nil {
		return nil, fmt.Errorf("kvdb: read: %w", err)
	}

	storedCRC := binary.LittleEndian.Uint32(buf[0:4])
	computedCRC := crc32.ChecksumIEEE(buf[4:])
	if storedCRC != computedCRC {
		return nil, ErrCorrupt
	}

	valStart := headerSize + len(key)
	return append([]byte(nil), buf[valStart:]...), nil
}

// Delete removes a key. Deleting an absent key is a no-op.
func (e *Engine) Delete(key []byte) error {
	if err := validateUserKey(key); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}
	if e.readOnly {
		return ErrReadOnly
	}
	if e.broken {
		return ErrBroken
	}

	// Deleting an absent key is a no-op.
	if _, ok := e.index[string(key)]; !ok {
		return nil
	}

	return e.deleteLocked(key, nowTimestamp())
}

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

// Batch applies a list of operations atomically.
func (e *Engine) Batch(ops []BatchOp) error {
	if len(ops) == 0 {
		return ErrBatchEmpty
	}
	if len(ops) > maxBatchOps {
		return ErrBatchTooLarge
	}
	for _, op := range ops {
		if err := validateUserKey(op.Key); err != nil {
			return err
		}
		if op.Value != nil && len(op.Value) > maxValueSize {
			return ErrValueTooLarge
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}
	if e.readOnly {
		return ErrReadOnly
	}
	if e.broken {
		return ErrBroken
	}

	ts := nowTimestamp()

	// Build the batch buffer:
	// [marker record][N normal/tombstone records]
	var buf bytes.Buffer

	// Marker record: key = batchMarkerKey, value = uint32 op count.
	markerVal := make([]byte, 4)
	binary.LittleEndian.PutUint32(markerVal, uint32(len(ops)))
	encodeRecord(&buf, batchMarkerKey, markerVal, ts)

	// Normal records.
	for _, op := range ops {
		if op.Value == nil {
			encodeTombstone(&buf, op.Key, ts)
		} else {
			encodeRecord(&buf, op.Key, op.Value, ts)
		}
	}

	data := buf.Bytes()
	oldOffset := e.offset

	if _, err := e.file.WriteAt(data, oldOffset); err != nil {
		_ = e.file.Truncate(oldOffset)
		return fmt.Errorf("kvdb: write batch: %w", err)
	}

	if err := e.file.Sync(); err != nil {
		e.broken = true
		return ErrBroken
	}

	// Fsync succeeded — update the index.
	e.offset = oldOffset + int64(len(data))

	// Parse through the buffer to compute offsets.
	pos := oldOffset
	// Skip the marker record.
	markerRecLen := int64(headerSize) + int64(len(batchMarkerKey)) + int64(len(markerVal))
	pos += markerRecLen

	for _, op := range ops {
		ks := int64(len(op.Key))
		if op.Value == nil {
			pos += int64(headerSize) + ks
			delete(e.index, string(op.Key))
		} else {
			vs := int64(len(op.Value))
			valPos := pos + int64(headerSize) + ks
			e.index[string(op.Key)] = indexEntry{
				fileID:    0,
				valueSize: int32(vs),
				valuePos:  valPos,
				timestamp: ts,
			}
			pos += int64(headerSize) + ks + vs
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Iteration / stats
// ---------------------------------------------------------------------------

// ForEach iterates over all live keys in unspecified order.
func (e *Engine) ForEach(fn func(key, value []byte) error) error {
	e.mu.RLock()
	type kv struct {
		key string
		ent indexEntry
	}
	snapshot := make([]kv, 0, len(e.index))
	for k, v := range e.index {
		snapshot = append(snapshot, kv{k, v})
	}
	e.mu.RUnlock()

	for _, item := range snapshot {
		val, err := e.Get([]byte(item.key))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := fn([]byte(item.key), val); err != nil {
			return err
		}
	}
	return nil
}

// Len returns the number of live keys.
func (e *Engine) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.index)
}

// Stats returns engine statistics.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Stats{
		Keys:     len(e.index),
		DataSize: e.offset,
		FilePath: e.path,
		ReadOnly: e.readOnly,
	}
}

// ---------------------------------------------------------------------------
// Compact
// ---------------------------------------------------------------------------

// Compact rewrites the database, removing dead entries.
func (e *Engine) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}
	if e.readOnly {
		return ErrReadOnly
	}
	if e.broken {
		return ErrBroken
	}

	compactPath := e.path + ".compact"
	_ = os.Remove(compactPath)

	cf, err := os.OpenFile(compactPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("kvdb: compact create: %w", err)
	}

	keys := make([]string, 0, len(e.index))
	for k := range e.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var offset int64
	newIndex := make(map[string]indexEntry, len(keys))

	for _, k := range keys {
		ent := e.index[k]
		key := []byte(k)

		value := make([]byte, ent.valueSize)
		if _, err := e.file.ReadAt(value, ent.valuePos); err != nil {
			cf.Close()
			os.Remove(compactPath)
			return fmt.Errorf("kvdb: compact read: %w", err)
		}

		var buf bytes.Buffer
		encodeRecord(&buf, key, value, ent.timestamp)
		data := buf.Bytes()

		if _, err := cf.WriteAt(data, offset); err != nil {
			cf.Close()
			os.Remove(compactPath)
			return fmt.Errorf("kvdb: compact write: %w", err)
		}

		newIndex[k] = indexEntry{
			fileID:    0,
			valueSize: ent.valueSize,
			valuePos:  offset + int64(headerSize) + int64(len(key)),
			timestamp: ent.timestamp,
		}
		offset += int64(len(data))
	}

	if err := cf.Sync(); err != nil {
		cf.Close()
		os.Remove(compactPath)
		return fmt.Errorf("kvdb: compact sync: %w", err)
	}

	// Close cf handle so Windows/POSIX can reopen or rename cleanly.
	if err := cf.Close(); err != nil {
		os.Remove(compactPath)
		return fmt.Errorf("kvdb: compact close: %w", err)
	}

	// Close oldFile handle before rename so Windows allows atomic rename over destination.
	// On Linux/POSIX os.Rename over an open file works, but closing first guarantees portability.
	oldFile := e.file
	if oldFile != nil {
		_ = oldFile.Close()
	}

	if err := os.Rename(compactPath, e.path); err != nil {
		// Attempt to restore oldFile handle if rename failed
		if rf, rerr := os.OpenFile(e.path, os.O_RDWR, 0644); rerr == nil {
			e.file = rf
		}
		os.Remove(compactPath)
		return fmt.Errorf("kvdb: compact rename: %w", err)
	}

	// Reopen the newly renamed file for reading and writing.
	newFile, err := os.OpenFile(e.path, os.O_RDWR, 0644)
	if err != nil {
		e.broken = true
		return fmt.Errorf("kvdb: compact reopen: %w", err)
	}

	// Fsync the directory.
	_ = syncDir(filepath.Dir(e.path))

	// Swap the handle and rebuild.
	e.file = newFile
	e.index = newIndex
	e.offset = offset

	return nil
}

// ---------------------------------------------------------------------------
// Internal: encoding
// ---------------------------------------------------------------------------

func encodeRecord(buf *bytes.Buffer, key, value []byte, ts uint32) {
	ks := uint32(len(key))
	vs := uint32(len(value))

	// Payload = everything after the CRC field.
	var payload bytes.Buffer
	_ = binary.Write(&payload, binary.LittleEndian, ts)
	_ = binary.Write(&payload, binary.LittleEndian, ks)
	_ = binary.Write(&payload, binary.LittleEndian, vs)
	payload.Write(key)
	payload.Write(value)

	crc := crc32.ChecksumIEEE(payload.Bytes())
	_ = binary.Write(buf, binary.LittleEndian, crc)
	buf.Write(payload.Bytes())
}

func encodeTombstone(buf *bytes.Buffer, key []byte, ts uint32) {
	ks := uint32(len(key))
	vs := uint32(tombstoneVal)

	var payload bytes.Buffer
	_ = binary.Write(&payload, binary.LittleEndian, ts)
	_ = binary.Write(&payload, binary.LittleEndian, ks)
	_ = binary.Write(&payload, binary.LittleEndian, vs)
	payload.Write(key)

	crc := crc32.ChecksumIEEE(payload.Bytes())
	_ = binary.Write(buf, binary.LittleEndian, crc)
	buf.Write(payload.Bytes())
}

// ---------------------------------------------------------------------------
// Internal: write under lock
// ---------------------------------------------------------------------------

func (e *Engine) putLocked(key, value []byte, ts uint32) error {
	if e.closed {
		return ErrClosed
	}
	if e.readOnly {
		return ErrReadOnly
	}
	if e.broken {
		return ErrBroken
	}

	var buf bytes.Buffer
	encodeRecord(&buf, key, value, ts)
	data := buf.Bytes()
	oldOffset := e.offset

	if _, err := e.file.WriteAt(data, oldOffset); err != nil {
		_ = e.file.Truncate(oldOffset)
		return fmt.Errorf("kvdb: write: %w", err)
	}

	if err := e.file.Sync(); err != nil {
		e.broken = true
		return ErrBroken
	}

	e.offset = oldOffset + int64(len(data))
	e.index[string(key)] = indexEntry{
		fileID:    0,
		valueSize: int32(len(value)),
		valuePos:  oldOffset + int64(headerSize) + int64(len(key)),
		timestamp: ts,
	}
	return nil
}

func (e *Engine) deleteLocked(key []byte, ts uint32) error {
	var buf bytes.Buffer
	encodeTombstone(&buf, key, ts)
	data := buf.Bytes()
	oldOffset := e.offset

	if _, err := e.file.WriteAt(data, oldOffset); err != nil {
		_ = e.file.Truncate(oldOffset)
		return fmt.Errorf("kvdb: write tombstone: %w", err)
	}

	if err := e.file.Sync(); err != nil {
		e.broken = true
		return ErrBroken
	}

	e.offset = oldOffset + int64(len(data))
	delete(e.index, string(key))
	return nil
}

// ---------------------------------------------------------------------------
// Internal: recovery
// ---------------------------------------------------------------------------

func (e *Engine) recover() (int64, error) {
	fi, err := e.file.Stat()
	if err != nil {
		return -1, fmt.Errorf("kvdb: stat: %w", err)
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		return -1, nil
	}

	var pos int64
	hdr := make([]byte, headerSize)
	truncateAt := int64(-1)

	for pos < fileSize {
		if pos+int64(headerSize) > fileSize {
			truncateAt = pos
			break
		}
		if _, err := e.file.ReadAt(hdr, pos); err != nil {
			truncateAt = pos
			break
		}

		storedCRC := binary.LittleEndian.Uint32(hdr[0:4])
		keySize := binary.LittleEndian.Uint32(hdr[8:12])
		valSize := binary.LittleEndian.Uint32(hdr[12:16])

		if keySize == 0 || keySize > maxKeySize {
			truncateAt = pos
			break
		}

		isTombstone := valSize == tombstoneVal
		var bodySize int64
		if isTombstone {
			bodySize = int64(keySize)
		} else {
			if valSize > maxValueSize {
				truncateAt = pos
				break
			}
			bodySize = int64(keySize) + int64(valSize)
		}

		recEnd := pos + int64(headerSize) + bodySize
		if recEnd > fileSize {
			truncateAt = pos
			break
		}

		// Read full payload (after CRC) for verification.
		payloadSize := 12 + bodySize // ts(4)+keySize(4)+valSize(4) + key + value
		fullPayload := make([]byte, payloadSize)
		copy(fullPayload[0:12], hdr[4:16])
		if bodySize > 0 {
			if _, err := e.file.ReadAt(fullPayload[12:], pos+int64(headerSize)); err != nil {
				truncateAt = pos
				break
			}
		}

		computedCRC := crc32.ChecksumIEEE(fullPayload)
		if storedCRC != computedCRC {
			truncateAt = pos
			break
		}

		key := fullPayload[12 : 12+keySize]

		// Check if this is a batch marker.
		if bytes.Equal(key, batchMarkerKey) {
			if isTombstone || valSize != 4 {
				truncateAt = pos
				break
			}
			opCount := binary.LittleEndian.Uint32(fullPayload[12+keySize:])
			batchOK, batchEnd := e.recoverBatch(recEnd, fileSize, opCount)
			if !batchOK {
				truncateAt = pos
				break
			}
			e.applyBatchToIndex(recEnd, opCount)
			pos = batchEnd
			e.offset = pos
			continue
		}

		ts := binary.LittleEndian.Uint32(hdr[4:8])
		if isTombstone {
			delete(e.index, string(key))
		} else {
			e.index[string(key)] = indexEntry{
				fileID:    0,
				valueSize: int32(valSize),
				valuePos:  pos + int64(headerSize) + int64(keySize),
				timestamp: ts,
			}
		}

		pos = recEnd
		e.offset = pos
	}

	return truncateAt, nil
}

func (e *Engine) recoverBatch(startPos, fileSize int64, opCount uint32) (bool, int64) {
	pos := startPos
	hdr := make([]byte, headerSize)

	for i := uint32(0); i < opCount; i++ {
		if pos+int64(headerSize) > fileSize {
			return false, 0
		}
		if _, err := e.file.ReadAt(hdr, pos); err != nil {
			return false, 0
		}

		storedCRC := binary.LittleEndian.Uint32(hdr[0:4])
		keySize := binary.LittleEndian.Uint32(hdr[8:12])
		valSize := binary.LittleEndian.Uint32(hdr[12:16])

		if keySize == 0 || keySize > maxKeySize {
			return false, 0
		}

		isTombstone := valSize == tombstoneVal
		var bodySize int64
		if isTombstone {
			bodySize = int64(keySize)
		} else {
			if valSize > maxValueSize {
				return false, 0
			}
			bodySize = int64(keySize) + int64(valSize)
		}

		recEnd := pos + int64(headerSize) + bodySize
		if recEnd > fileSize {
			return false, 0
		}

		payloadSize := 12 + bodySize
		fullPayload := make([]byte, payloadSize)
		copy(fullPayload[0:12], hdr[4:16])
		if bodySize > 0 {
			if _, err := e.file.ReadAt(fullPayload[12:], pos+int64(headerSize)); err != nil {
				return false, 0
			}
		}

		if crc32.ChecksumIEEE(fullPayload) != storedCRC {
			return false, 0
		}

		pos = recEnd
	}

	return true, pos
}

func (e *Engine) applyBatchToIndex(startPos int64, opCount uint32) {
	pos := startPos
	hdr := make([]byte, headerSize)

	for i := uint32(0); i < opCount; i++ {
		e.file.ReadAt(hdr, pos)

		ts := binary.LittleEndian.Uint32(hdr[4:8])
		keySize := binary.LittleEndian.Uint32(hdr[8:12])
		valSize := binary.LittleEndian.Uint32(hdr[12:16])

		isTombstone := valSize == tombstoneVal

		keyBuf := make([]byte, keySize)
		e.file.ReadAt(keyBuf, pos+int64(headerSize))

		if isTombstone {
			delete(e.index, string(keyBuf))
			pos += int64(headerSize) + int64(keySize)
		} else {
			e.index[string(keyBuf)] = indexEntry{
				fileID:    0,
				valueSize: int32(valSize),
				valuePos:  pos + int64(headerSize) + int64(keySize),
				timestamp: ts,
			}
			pos += int64(headerSize) + int64(keySize) + int64(valSize)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func validateUserKey(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > maxKeySize {
		return ErrKeyTooLarge
	}
	if bytes.Equal(key, batchMarkerKey) {
		return ErrReservedKey
	}
	return nil
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	err = d.Sync()
	// On Windows, syncing a directory handle returns EACCES ("Access is denied.")
	// Ignore this error on systems where syncing directories is not supported.
	if err != nil && errors.Is(err, os.ErrPermission) {
		return nil
	}
	return err
}

func nowTimestamp() uint32 {
	return uint32(time.Now().Unix())
}
