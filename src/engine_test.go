package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// helper: create a temp DB path
func tempDB(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test.db")
}

// ---------------------------------------------------------------------------
// Reopen persistence
// ---------------------------------------------------------------------------

func TestReopenPersistence(t *testing.T) {
	path := tempDB(t)

	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := e.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := e.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := e.Delete([]byte("foo")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	e.Close()

	// Reopen.
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	val, err := e2.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}

	_, err = e2.Get([]byte("foo"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for deleted key, got %v", err)
	}

	if e2.Len() != 1 {
		t.Fatalf("expected 1 key, got %d", e2.Len())
	}
}

// ---------------------------------------------------------------------------
// Torn tail
// ---------------------------------------------------------------------------

func TestTornTail(t *testing.T) {
	path := tempDB(t)

	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := e.Put([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := e.Put([]byte("key2"), []byte("val2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	goodOffset := e.offset
	if err := e.Put([]byte("key3"), []byte("val3")); err != nil {
		t.Fatalf("put: %v", err)
	}
	e.Close()

	// Simulate a torn tail by truncating the last record partially.
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	f.Truncate(goodOffset + 5) // partial header of key3
	f.Close()

	// Reopen — should recover key1 and key2, truncate the rest.
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer e2.Close()

	if e2.Len() != 2 {
		t.Fatalf("expected 2 keys after torn tail recovery, got %d", e2.Len())
	}

	val, _ := e2.Get([]byte("key1"))
	if string(val) != "val1" {
		t.Fatalf("key1 wrong value: %q", val)
	}
	val, _ = e2.Get([]byte("key2"))
	if string(val) != "val2" {
		t.Fatalf("key2 wrong value: %q", val)
	}
	_, err = e2.Get([]byte("key3"))
	if err != ErrNotFound {
		t.Fatalf("key3 should be not found")
	}
}

// ---------------------------------------------------------------------------
// Every-prefix recovery: no Open error for any truncation length
// ---------------------------------------------------------------------------

func TestEveryPrefixRecovery(t *testing.T) {
	path := tempDB(t)

	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := e.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := e.Put([]byte("b"), []byte("22")); err != nil {
		t.Fatalf("put: %v", err)
	}
	fullSize := e.offset
	e.Close()

	// Read the full data.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	for cutLen := int64(0); cutLen <= fullSize; cutLen++ {
		// Write truncated data.
		cpath := filepath.Join(t.TempDir(), "test.db")
		os.WriteFile(cpath, data[:cutLen], 0644)

		eng, err := Open(cpath)
		if err != nil {
			t.Fatalf("Open failed at cutLen=%d: %v", cutLen, err)
		}
		eng.Close()
	}
}

// ---------------------------------------------------------------------------
// Batch atomicity at every cut point
// ---------------------------------------------------------------------------

func TestBatchAtomicityEveryByteCut(t *testing.T) {
	path := tempDB(t)

	// Write a key before the batch so we have something to check.
	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := e.Put([]byte("pre"), []byte("exists")); err != nil {
		t.Fatalf("put: %v", err)
	}
	preOffset := e.offset

	// Write a batch.
	ops := []BatchOp{
		{Key: []byte("bk1"), Value: []byte("bv1")},
		{Key: []byte("bk2"), Value: []byte("bv2")},
		{Key: []byte("bk3"), Value: nil}, // delete
	}
	if err := e.Batch(ops); err != nil {
		t.Fatalf("batch: %v", err)
	}
	fullSize := e.offset
	e.Close()

	data, _ := os.ReadFile(path)

	// Cut at every byte from just after the "pre" record to the end.
	for cutLen := preOffset; cutLen < fullSize; cutLen++ {
		cpath := filepath.Join(t.TempDir(), "test.db")
		os.WriteFile(cpath, data[:cutLen], 0644)

		eng, err := Open(cpath)
		if err != nil {
			t.Fatalf("Open at cut=%d: %v", cutLen, err)
		}

		// The pre-batch key should always be there.
		val, err := eng.Get([]byte("pre"))
		if err != nil {
			t.Fatalf("cut=%d: pre key missing: %v", cutLen, err)
		}
		if string(val) != "exists" {
			t.Fatalf("cut=%d: pre wrong value: %q", cutLen, val)
		}

		// No batch keys should be visible (all-or-nothing).
		_, err1 := eng.Get([]byte("bk1"))
		_, err2 := eng.Get([]byte("bk2"))
		if err1 != ErrNotFound || err2 != ErrNotFound {
			t.Fatalf("cut=%d: partial batch visible! bk1=%v bk2=%v", cutLen, err1, err2)
		}
		eng.Close()
	}

	// At full length, the batch should be visible.
	cpath := filepath.Join(t.TempDir(), "test.db")
	os.WriteFile(cpath, data, 0644)
	eng, _ := Open(cpath)
	v1, _ := eng.Get([]byte("bk1"))
	v2, _ := eng.Get([]byte("bk2"))
	if string(v1) != "bv1" || string(v2) != "bv2" {
		t.Fatalf("full batch should be visible")
	}
	eng.Close()
}

// ---------------------------------------------------------------------------
// CRC bit-rot on Get
// ---------------------------------------------------------------------------

func TestCRCBitRotOnGet(t *testing.T) {
	path := tempDB(t)

	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := e.Put([]byte("mykey"), []byte("myvalue")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Corrupt a byte in the value area.
	ent := e.index["mykey"]
	corruptPos := ent.valuePos + 1 // second byte of value
	buf := make([]byte, 1)
	e.file.ReadAt(buf, corruptPos)
	buf[0] ^= 0xFF
	e.file.WriteAt(buf, corruptPos)
	e.file.Sync()

	_, err = e.Get([]byte("mykey"))
	if err != ErrCorrupt {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
	e.Close()
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

func TestCompaction(t *testing.T) {
	path := tempDB(t)

	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write many versions of the same key.
	for i := 0; i < 100; i++ {
		if err := e.Put([]byte("key"), []byte(fmt.Sprintf("val%d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	// Write some other keys.
	if err := e.Put([]byte("other"), []byte("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := e.Delete([]byte("other")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	sizeBefore := e.offset

	if err := e.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	sizeAfter := e.offset
	if sizeAfter >= sizeBefore {
		t.Fatalf("compaction didn't shrink: before=%d after=%d", sizeBefore, sizeAfter)
	}

	// Verify the data.
	val, err := e.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get after compact: %v", err)
	}
	if string(val) != "val99" {
		t.Fatalf("expected val99, got %q", val)
	}

	_, err = e.Get([]byte("other"))
	if err != ErrNotFound {
		t.Fatalf("deleted key should still be gone after compact")
	}

	if e.Len() != 1 {
		t.Fatalf("expected 1 key, got %d", e.Len())
	}

	e.Close()

	// Reopen to verify compact file is valid.
	e2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after compact: %v", err)
	}
	defer e2.Close()

	val, _ = e2.Get([]byte("key"))
	if string(val) != "val99" {
		t.Fatalf("wrong value after reopen: %q", val)
	}
}

// ---------------------------------------------------------------------------
// Read-only open doesn't modify
// ---------------------------------------------------------------------------

func TestReadOnlyOpenDoesNotModify(t *testing.T) {
	path := tempDB(t)

	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := e.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	goodOffset := e.offset
	// Write a partial record to simulate a torn tail.
	if err := e.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	e.Close()

	// Truncate to create a torn tail.
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	f.Truncate(goodOffset + 3)
	f.Close()

	// Get file info before read-only open.
	infoBefore, _ := os.Stat(path)
	sizeBefore := infoBefore.Size()

	// Read-only open.
	e2, err := OpenWithOptions(path, Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}

	// Verify it can read the valid data.
	val, err := e2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("wrong value: %q", val)
	}

	// Writes should fail.
	if err := e2.Put([]byte("x"), []byte("y")); err != ErrReadOnly {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}

	e2.Close()

	// Verify file was not modified.
	infoAfter, _ := os.Stat(path)
	if infoAfter.Size() != sizeBefore {
		t.Fatalf("file size changed: %d -> %d", sizeBefore, infoAfter.Size())
	}
}

// ---------------------------------------------------------------------------
// Concurrent Put/Get under -race
// ---------------------------------------------------------------------------

func TestConcurrentPutGet(t *testing.T) {
	path := tempDB(t)

	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	const numWriters = 4
	const numReaders = 4
	const opsPerWorker = 100

	var wg sync.WaitGroup

	// Writers.
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", id, i)
				val := fmt.Sprintf("v%d-%d", id, i)
				if err := e.Put([]byte(key), []byte(val)); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(w)
	}

	// Readers.
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker*2; i++ {
				// Read any key; not-found is fine.
				key := fmt.Sprintf("w%d-k%d", i%numWriters, i%opsPerWorker)
				_, err := e.Get([]byte(key))
				if err != nil && err != ErrNotFound {
					t.Errorf("get: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// Reserved key rejection
// ---------------------------------------------------------------------------

func TestReservedKeyRejection(t *testing.T) {
	path := tempDB(t)
	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	if err := e.Put(batchMarkerKey, []byte("hack")); err != ErrReservedKey {
		t.Fatalf("expected ErrReservedKey, got %v", err)
	}
	if err := e.Delete(batchMarkerKey); err != ErrReservedKey {
		t.Fatalf("expected ErrReservedKey, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Delete absent key is no-op
// ---------------------------------------------------------------------------

func TestDeleteAbsentKeyIsNoop(t *testing.T) {
	path := tempDB(t)
	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer e.Close()

	offsetBefore := e.offset
	if err := e.Delete([]byte("nonexistent")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if e.offset != offsetBefore {
		t.Fatalf("offset changed for absent delete: %d -> %d", offsetBefore, e.offset)
	}
}

// ---------------------------------------------------------------------------
// Serve protocol tests
// ---------------------------------------------------------------------------

func TestServeBasicProtocol(t *testing.T) {
	path := tempDB(t)

	input := strings.Join([]string{
		"1 PUT mykey myvalue",
		"2 GET mykey",
		"3 DEL mykey",
		"4 GET mykey",
		"5 COMPACT",
	}, "\n") + "\n"

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "serve"}, strings.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("serve exited %d: %s", code, stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	expected := []string{
		"ACK 1",
		"VAL 2 myvalue",
		"ACK 3",
		"NOTFOUND 4",
		"ACK 5",
	}

	if len(lines) != len(expected) {
		t.Fatalf("expected %d lines, got %d: %v", len(expected), len(lines), lines)
	}
	for i, exp := range expected {
		if lines[i] != exp {
			t.Errorf("line %d: expected %q, got %q", i, exp, lines[i])
		}
	}
}

func TestServeMalformedLines(t *testing.T) {
	path := tempDB(t)

	input := strings.Join([]string{
		"",          // empty line
		"1",         // no command
		"2 UNKNOWN", // unknown command
		"3 PUT",     // missing key+value
		"4 GET",     // missing key
		"5 DEL",     // missing key
	}, "\n") + "\n"

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "serve"}, strings.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("serve exited %d: %s", code, stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "ERR ") {
			t.Errorf("expected ERR, got %q", line)
		}
	}
}

func TestServeBatchErrors(t *testing.T) {
	path := tempDB(t)

	input := strings.Join([]string{
		"1 BATCH 2",
		"PUT k1 v1",
		"INVALID LINE",
		"2 GET k1",
	}, "\n") + "\n"

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "serve"}, strings.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("serve exited %d: %s", code, stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %v", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0], "ERR 1") {
		t.Errorf("expected ERR for batch, got %q", lines[0])
	}
	if lines[1] != "NOTFOUND 2" {
		t.Errorf("expected NOTFOUND, got %q", lines[1])
	}
}

func TestServeEOFMidBatch(t *testing.T) {
	path := tempDB(t)

	// BATCH 3, but only 1 sub-line before EOF.
	input := "1 BATCH 3\nPUT k1 v1\n"

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "serve"}, strings.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("serve exited %d: %s", code, stderr.String())
	}

	// EOF mid-batch: apply nothing, send no reply.
	output := strings.TrimSpace(stdout.String())
	if output != "" {
		t.Fatalf("expected no output for EOF mid-batch, got %q", output)
	}
}

func TestServe900KBValue(t *testing.T) {
	path := tempDB(t)

	bigValue := strings.Repeat("X", 900*1024)
	input := fmt.Sprintf("1 PUT bigkey %s\n2 GET bigkey\n", bigValue)

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "serve"}, strings.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("serve exited %d: %s", code, stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if lines[0] != "ACK 1" {
		t.Errorf("expected ACK 1, got %q", lines[0])
	}
	expectedPrefix := "VAL 2 "
	if !strings.HasPrefix(lines[1], expectedPrefix) {
		t.Errorf("expected VAL 2 prefix, got %q", lines[1][:min(50, len(lines[1]))])
	}
	gotVal := lines[1][len(expectedPrefix):]
	if len(gotVal) != 900*1024 {
		t.Errorf("expected %d bytes, got %d", 900*1024, len(gotVal))
	}
}

// ---------------------------------------------------------------------------
// Serve BATCH success path
// ---------------------------------------------------------------------------

func TestServeBatchSuccess(t *testing.T) {
	path := tempDB(t)

	input := strings.Join([]string{
		"1 BATCH 2",
		"PUT bk1 bv1",
		"PUT bk2 bv2",
		"2 GET bk1",
		"3 GET bk2",
	}, "\n") + "\n"

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "serve"}, strings.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("serve exited %d: %s", code, stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	expected := []string{"ACK 1", "VAL 2 bv1", "VAL 3 bv2"}
	if len(lines) != len(expected) {
		t.Fatalf("expected %d lines, got %d: %v", len(expected), len(lines), lines)
	}
	for i, exp := range expected {
		if lines[i] != exp {
			t.Errorf("line %d: expected %q, got %q", i, exp, lines[i])
		}
	}
}

// ---------------------------------------------------------------------------
// CLI put/get/del/dump/check
// ---------------------------------------------------------------------------

func TestCLIPutGetDel(t *testing.T) {
	path := tempDB(t)

	var stdout, stderr bytes.Buffer

	code := run([]string{"kvdb", path, "put", "cli-key", "cli-val"}, nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("put exited %d: %s", code, stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"kvdb", path, "get", "cli-key"}, nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("get exited %d: %s", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "cli-val" {
		t.Fatalf("expected 'cli-val', got %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"kvdb", path, "del", "cli-key"}, nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("del exited %d: %s", code, stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"kvdb", path, "get", "cli-key"}, nil, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("get should exit 1 for missing key, got %d", code)
	}
}

func TestCLIDump(t *testing.T) {
	path := tempDB(t)
	e, _ := Open(path)
	e.Put([]byte("bravo"), []byte("2"))
	e.Put([]byte("alpha"), []byte("1"))
	e.Put([]byte("charlie"), []byte("3"))
	e.Close()

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "dump"}, nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dump exited %d: %s", code, stderr.String())
	}

	expected := "alpha\t1\nbravo\t2\ncharlie\t3\n"
	if stdout.String() != expected {
		t.Fatalf("dump output mismatch:\ngot:  %q\nwant: %q", stdout.String(), expected)
	}
}

func TestCLICheck(t *testing.T) {
	path := tempDB(t)
	e, _ := Open(path)
	e.Put([]byte("k"), []byte("v"))
	e.Close()

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "check"}, nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("check should pass, exited %d: %s", code, stderr.String())
	}

	// Corrupt the tail.
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	fi, _ := f.Stat()
	f.Truncate(fi.Size() - 2)
	f.Close()

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"kvdb", path, "check"}, nil, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("check should fail for corrupt tail, exited %d", code)
	}
}

// ---------------------------------------------------------------------------
// Batch with Delete in batch
// ---------------------------------------------------------------------------

func TestBatchWithDelete(t *testing.T) {
	path := tempDB(t)
	e, _ := Open(path)

	e.Put([]byte("existing"), []byte("val"))

	ops := []BatchOp{
		{Key: []byte("new1"), Value: []byte("v1")},
		{Key: []byte("existing"), Value: nil}, // delete
	}
	if err := e.Batch(ops); err != nil {
		t.Fatalf("batch: %v", err)
	}

	v, err := e.Get([]byte("new1"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("new1: %v %q", err, v)
	}
	_, err = e.Get([]byte("existing"))
	if err != ErrNotFound {
		t.Fatalf("existing should be deleted")
	}

	e.Close()

	// Reopen and verify.
	e2, _ := Open(path)
	defer e2.Close()
	v, _ = e2.Get([]byte("new1"))
	if string(v) != "v1" {
		t.Fatalf("new1 after reopen: %q", v)
	}
	_, err = e2.Get([]byte("existing"))
	if err != ErrNotFound {
		t.Fatalf("existing should be deleted after reopen")
	}
}

// ---------------------------------------------------------------------------
// ForEach
// ---------------------------------------------------------------------------

func TestForEach(t *testing.T) {
	path := tempDB(t)
	e, _ := Open(path)
	defer e.Close()

	e.Put([]byte("a"), []byte("1"))
	e.Put([]byte("b"), []byte("2"))
	e.Put([]byte("c"), []byte("3"))
	e.Delete([]byte("b"))

	count := 0
	e.ForEach(func(key, value []byte) error {
		count++
		return nil
	})
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

func TestStats(t *testing.T) {
	path := tempDB(t)
	e, _ := Open(path)
	defer e.Close()

	e.Put([]byte("k"), []byte("v"))
	st := e.Stats()
	if st.Keys != 1 {
		t.Fatalf("expected 1 key, got %d", st.Keys)
	}
	if st.DataSize == 0 {
		t.Fatalf("expected non-zero data size")
	}
	if st.ReadOnly {
		t.Fatalf("should not be read-only")
	}
}

// ---------------------------------------------------------------------------
// Encoding correctness: manually verify CRC
// ---------------------------------------------------------------------------

func TestEncodingCRC(t *testing.T) {
	var buf bytes.Buffer
	encodeRecord(&buf, []byte("key"), []byte("value"), 12345)
	data := buf.Bytes()

	storedCRC := binary.LittleEndian.Uint32(data[0:4])
	computed := crc32.ChecksumIEEE(data[4:])
	if storedCRC != computed {
		t.Fatalf("CRC mismatch: stored=%x computed=%x", storedCRC, computed)
	}

	// Verify header fields.
	ts := binary.LittleEndian.Uint32(data[4:8])
	ks := binary.LittleEndian.Uint32(data[8:12])
	vs := binary.LittleEndian.Uint32(data[12:16])
	if ts != 12345 {
		t.Fatalf("timestamp: %d", ts)
	}
	if ks != 3 {
		t.Fatalf("keySize: %d", ks)
	}
	if vs != 5 {
		t.Fatalf("valSize: %d", vs)
	}
	if string(data[16:19]) != "key" {
		t.Fatalf("key mismatch")
	}
	if string(data[19:24]) != "value" {
		t.Fatalf("value mismatch")
	}
}

// ---------------------------------------------------------------------------
// Tombstone encoding
// ---------------------------------------------------------------------------

func TestTombstoneEncoding(t *testing.T) {
	var buf bytes.Buffer
	encodeTombstone(&buf, []byte("del"), 999)
	data := buf.Bytes()

	vs := binary.LittleEndian.Uint32(data[12:16])
	if vs != tombstoneVal {
		t.Fatalf("expected tombstone marker, got %x", vs)
	}
	// No value bytes after the key.
	expectedLen := headerSize + 3 // "del"
	if len(data) != expectedLen {
		t.Fatalf("expected %d bytes, got %d", expectedLen, len(data))
	}
}

// ---------------------------------------------------------------------------
// Serve: value with spaces
// ---------------------------------------------------------------------------

func TestServePutValueWithSpaces(t *testing.T) {
	path := tempDB(t)
	input := "1 PUT mykey hello world foo\n2 GET mykey\n"

	var stdout, stderr bytes.Buffer
	code := run([]string{"kvdb", path, "serve"}, strings.NewReader(input), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if lines[1] != "VAL 2 hello world foo" {
		t.Fatalf("expected 'VAL 2 hello world foo', got %q", lines[1])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
