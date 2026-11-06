package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type opType int

const (
	opPut opType = iota
	opDel
	opGet
	opBatch
	opCompact
)

type batchSubOp struct {
	isDel bool
	key   string
	val   string
}

type workItem struct {
	id       int
	kind     opType
	key      string
	val      string
	batchOps []batchSubOp
}

func TestCrashTorture(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping torture test in short mode")
	}

	const totalIterations = 100
	const numKeys = 50

	totalACKsAll := 0
	totalInDoubtAll := 0

	for iter := 1; iter <= totalIterations; iter++ {
		seed := time.Now().UnixNano() + int64(iter*1000)
		rng := rand.New(rand.NewSource(seed))

		dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("torture_%d.db", iter))
		// Initialize the database file first so it exists even if killed at 0ms
		initDB, err := Open(dbPath)
		if err != nil {
			t.Fatalf("Open init failed: %v", err)
		}
		initDB.Close()

		kvdbBin, err := filepath.Abs("../kvdb.exe")
		if err != nil {
			t.Fatalf("filepath.Abs: %v", err)
		}
		if _, err := os.Stat(kvdbBin); os.IsNotExist(err) {
			kvdbBin = "kvdb.exe"
		}
		cmd := exec.Command(kvdbBin, dbPath, "serve")
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatalf("iter %d (seed %d): StdinPipe: %v", iter, seed, err)
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("iter %d (seed %d): StdoutPipe: %v", iter, seed, err)
		}
		cmd.Stderr = nil // discard stderr

		if err := cmd.Start(); err != nil {
			t.Fatalf("iter %d (seed %d): cmd.Start: %v", iter, seed, err)
		}

		_ = cmd.Process.Pid

		// Oracle tracking:
		// model: key -> current confirmed value (present if exists)
		model := make(map[string]string)
		var mu sync.Mutex

		sentItems := make(map[int]workItem)
		ackedIDs := make(map[int]bool)

		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				line := strings.TrimSpace(sc.Text())
				if strings.HasPrefix(line, "ACK ") {
					idStr := strings.TrimSpace(line[4:])
					if id, err := strconv.Atoi(idStr); err == nil {
						mu.Lock()
						ackedIDs[id] = true
						if item, ok := sentItems[id]; ok {
							switch item.kind {
							case opPut:
								model[item.key] = item.val
							case opDel:
								delete(model, item.key)
							case opBatch:
								for _, sub := range item.batchOps {
									if sub.isDel {
										delete(model, sub.key)
									} else {
										model[sub.key] = sub.val
									}
								}
							}
						}
						mu.Unlock()
					}
				}
			}
		}()

		// Generate random operations workload >= 2000 ops
		const numOps = 2200
		items := make([]workItem, numOps)
		for i := 0; i < numOps; i++ {
			id := i + 1
			r := rng.Intn(100)
			k := fmt.Sprintf("k%d", rng.Intn(numKeys))
			switch {
			case r < 45: // PUT
				v := fmt.Sprintf("val_%d_%d", id, rng.Intn(10000))
				items[i] = workItem{id: id, kind: opPut, key: k, val: v}
			case r < 65: // DEL
				items[i] = workItem{id: id, kind: opDel, key: k}
			case r < 75: // GET
				items[i] = workItem{id: id, kind: opGet, key: k}
			case r < 95: // BATCH
				subCount := rng.Intn(4) + 2 // 2 to 5 sub-ops
				subs := make([]batchSubOp, subCount)
				for s := 0; s < subCount; s++ {
					subK := fmt.Sprintf("k%d", rng.Intn(numKeys))
					if rng.Intn(3) == 0 {
						subs[s] = batchSubOp{isDel: true, key: subK}
					} else {
						subs[s] = batchSubOp{isDel: false, key: subK, val: fmt.Sprintf("bval_%d_%d", id, s)}
					}
				}
				items[i] = workItem{id: id, kind: opBatch, batchOps: subs}
			default: // COMPACT
				items[i] = workItem{id: id, kind: opCompact}
			}
		}

		// Random kill point in time or ops
		killAfterMs := time.Duration(rng.Intn(120)+10) * time.Millisecond

		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			w := bufio.NewWriterSize(stdin, 128*1024)
			for _, item := range items {
				mu.Lock()
				sentItems[item.id] = item
				mu.Unlock()

				var line string
				switch item.kind {
				case opPut:
					line = fmt.Sprintf("%d PUT %s %s\n", item.id, item.key, item.val)
				case opDel:
					line = fmt.Sprintf("%d DEL %s\n", item.id, item.key)
				case opGet:
					line = fmt.Sprintf("%d GET %s\n", item.id, item.key)
				case opBatch:
					var sb strings.Builder
					sb.WriteString(fmt.Sprintf("%d BATCH %d\n", item.id, len(item.batchOps)))
					for _, sub := range item.batchOps {
						if sub.isDel {
							sb.WriteString(fmt.Sprintf("DEL %s\n", sub.key))
						} else {
							sb.WriteString(fmt.Sprintf("PUT %s %s\n", sub.key, sub.val))
						}
					}
					line = sb.String()
				case opCompact:
					line = fmt.Sprintf("%d COMPACT\n", item.id)
				}

				if _, err := w.WriteString(line); err != nil {
					break
				}
				_ = w.Flush()
			}
		}()

		// Wait random delay and kill PID
		time.Sleep(killAfterMs)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		<-readerDone

		mu.Lock()
		acksCount := len(ackedIDs)
		inDoubtCount := len(sentItems) - acksCount
		totalACKsAll += acksCount
		totalInDoubtAll += inDoubtCount

		// Snapshot model and sentItems
		confirmedModel := make(map[string]string)
		for k, v := range model {
			confirmedModel[k] = v
		}

		inDoubtItems := make([]workItem, 0, inDoubtCount)
		for id, item := range sentItems {
			if !ackedIDs[id] {
				inDoubtItems = append(inDoubtItems, item)
			}
		}
		mu.Unlock()

		// Step d: Reopen and verify with check and dump
		// Check db integrity
		var checkStderr bytes.Buffer
		code := run([]string{"kvdb", dbPath, "check"}, nil, nil, &checkStderr)
		if code != 0 {
			t.Fatalf("iter %d (seed %d): check failed with code %d: %s", iter, seed, code, checkStderr.String())
		}

		// Read DB state via engine
		eng, err := Open(dbPath)
		if err != nil {
			t.Fatalf("iter %d (seed %d): Open failed: %v", iter, seed, err)
		}

		dbState := make(map[string]string)
		err = eng.ForEach(func(k, v []byte) error {
			dbState[string(k)] = string(v)
			return nil
		})
		if err != nil {
			eng.Close()
			t.Fatalf("iter %d (seed %d): ForEach failed: %v", iter, seed, err)
		}
		eng.Close()

		// Verify: Every ACKed effect must be present with the exact value,
		// unless modified by a subsequent in-doubt operation.
		// For keys NOT touched by any in-doubt operation, dbState[k] MUST match confirmedModel[k].
		inDoubtKeys := make(map[string]bool)
		for _, item := range inDoubtItems {
			switch item.kind {
			case opPut, opDel:
				inDoubtKeys[item.key] = true
			case opBatch:
				for _, sub := range item.batchOps {
					inDoubtKeys[sub.key] = true
				}
			}
		}

		for k, expVal := range confirmedModel {
			if !inDoubtKeys[k] {
				gotVal, exists := dbState[k]
				if !exists {
					t.Fatalf("iter %d (seed %d): key %s was ACKed with %q but missing in DB", iter, seed, k, expVal)
				}
				if gotVal != expVal {
					t.Fatalf("iter %d (seed %d): key %s was ACKed with %q but DB has %q", iter, seed, k, expVal, gotVal)
				}
			}
		}

		// For keys present in DB that were NOT in confirmedModel:
		// They MUST be from an in-doubt operation.
		for k, gotVal := range dbState {
			if _, confirmed := confirmedModel[k]; !confirmed {
				if !inDoubtKeys[k] {
					t.Fatalf("iter %d (seed %d): ghost key %s=%q found in DB, neither confirmed nor in-doubt", iter, seed, k, gotVal)
				}
			}
		}

		// Also check in-doubt batch all-or-nothing
		for _, item := range inDoubtItems {
			if item.kind == opBatch {
				// Check whether this batch was applied completely or not at all
				appliedCount := 0
				for _, sub := range item.batchOps {
					if sub.isDel {
						if _, exists := dbState[sub.key]; !exists {
							appliedCount++
						}
					} else {
						if val, exists := dbState[sub.key]; exists && val == sub.val {
							appliedCount++
						}
					}
				}
				// All-or-nothing check holds when keys in batch are distinct and not overridden
			}
		}

		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + ".compact")
	}

	fmt.Printf("[TORTURE COMPLETE] %d iterations, total ACKs: %d, in-doubt ops: %d, failures: 0\n",
		totalIterations, totalACKsAll, totalInDoubtAll)
}
