package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

func main() {
	os.Exit(run(os.Args, os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) < 3 {
		fmt.Fprintln(stderr, "usage: kvdb <db-path> <command> [args...]")
		return 2
	}

	dbPath := args[1]
	cmd := args[2]

	switch cmd {
	case "put":
		return cmdPut(dbPath, args[3:], stderr)
	case "get":
		return cmdGet(dbPath, args[3:], stdout, stderr)
	case "del":
		return cmdDel(dbPath, args[3:], stderr)
	case "batch":
		return cmdBatch(dbPath, args[3:], stdin, stderr)
	case "dump":
		return cmdDump(dbPath, stdout, stderr)
	case "check":
		return cmdCheck(dbPath, stderr)
	case "compact":
		return cmdCompact(dbPath, stderr)
	case "serve":
		return cmdServe(dbPath, stdin, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", cmd)
		return 2
	}
}

func cmdPut(dbPath string, args []string, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprintln(stderr, "usage: kvdb <db-path> put <key> <value>")
		return 2
	}
	e, err := Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer e.Close()

	if err := e.Put([]byte(args[0]), []byte(args[1])); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func cmdGet(dbPath string, args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: kvdb <db-path> get <key>")
		return 2
	}
	e, err := Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer e.Close()

	val, err := e.Get([]byte(args[0]))
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, string(val))
	return 0
}

func cmdDel(dbPath string, args []string, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: kvdb <db-path> del <key>")
		return 2
	}
	e, err := Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer e.Close()

	if err := e.Delete([]byte(args[0])); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func cmdBatch(dbPath string, args []string, stdin io.Reader, stderr io.Writer) int {
	e, err := Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer e.Close()

	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var ops []BatchOp
	for scanner.Scan() {
		line := scanner.Text()
		op, err := parseBatchLine(line)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		ops = append(ops, op)
	}
	if scanner.Err() != nil {
		fmt.Fprintf(stderr, "error: %v\n", scanner.Err())
		return 1
	}

	if len(ops) == 0 {
		return 0
	}

	if err := e.Batch(ops); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func cmdDump(dbPath string, stdout, stderr io.Writer) int {
	e, err := Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer e.Close()

	type kv struct {
		key   string
		value string
	}

	var pairs []kv
	err = e.ForEach(func(key, value []byte) error {
		pairs = append(pairs, kv{string(key), string(value)})
		return nil
	})
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].key < pairs[j].key
	})

	for _, p := range pairs {
		fmt.Fprintf(stdout, "%s\t%s\n", p.key, p.value)
	}
	return 0
}

func cmdCheck(dbPath string, stderr io.Writer) int {
	e, err := OpenWithOptions(dbPath, Options{ReadOnly: true})
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer e.Close()

	if e.hasTornTail {
		fmt.Fprintf(stderr, "torn/corrupt tail at offset %d\n", e.offset)
		return 1
	}
	return 0
}

func cmdCompact(dbPath string, stderr io.Writer) int {
	e, err := Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer e.Close()

	if err := e.Compact(); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// serve
// ---------------------------------------------------------------------------

func cmdServe(dbPath string, stdin io.Reader, stdout, stderr io.Writer) int {
	e, err := Open(dbPath)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer e.Close()

	w := bufio.NewWriter(stdout)
	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r\n")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			fmt.Fprintf(w, "ERR - empty line\n")
			w.Flush()
			continue
		}

		parts := strings.SplitN(trimmed, " ", 3)
		if len(parts) < 2 {
			fmt.Fprintf(w, "ERR - invalid command\n")
			w.Flush()
			continue
		}

		id := parts[0]
		verb := parts[1]
		rest := ""
		if len(parts) > 2 {
			rest = parts[2]
		}

		switch verb {
		case "PUT":
			servePut(e, id, rest, w)
		case "DEL":
			serveDel(e, id, rest, w)
		case "GET":
			serveGet(e, id, rest, w)
		case "BATCH":
			serveBatch(e, id, rest, scanner, w)
		case "COMPACT":
			serveCompact(e, id, rest, w)
		default:
			fmt.Fprintf(w, "ERR %s unknown command\n", id)
			w.Flush()
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(w, "ERR - scanner overflow: %v\n", err)
		w.Flush()
		fmt.Fprintf(stderr, "error: scanner: %v\n", err)
		return 1
	}

	return 0
}

func servePut(e *Engine, id, rest string, w *bufio.Writer) {
	// rest = "<key> <value>" where value is rest of line
	idx := strings.IndexByte(rest, ' ')
	if idx < 0 || idx == 0 {
		fmt.Fprintf(w, "ERR %s invalid PUT syntax\n", id)
		w.Flush()
		return
	}
	key := rest[:idx]
	value := rest[idx+1:]

	if err := e.Put([]byte(key), []byte(value)); err != nil {
		fmt.Fprintf(w, "ERR %s %s\n", id, err.Error())
		w.Flush()
		return
	}
	fmt.Fprintf(w, "ACK %s\n", id)
	w.Flush()
}

func serveDel(e *Engine, id, rest string, w *bufio.Writer) {
	key := strings.TrimSpace(rest)
	if key == "" {
		fmt.Fprintf(w, "ERR %s missing key\n", id)
		w.Flush()
		return
	}
	if strings.Contains(key, " ") {
		fmt.Fprintf(w, "ERR %s extra tokens\n", id)
		w.Flush()
		return
	}
	if err := e.Delete([]byte(key)); err != nil {
		fmt.Fprintf(w, "ERR %s %s\n", id, err.Error())
		w.Flush()
		return
	}
	fmt.Fprintf(w, "ACK %s\n", id)
	w.Flush()
}

func serveGet(e *Engine, id, rest string, w *bufio.Writer) {
	key := strings.TrimSpace(rest)
	if key == "" {
		fmt.Fprintf(w, "ERR %s missing key\n", id)
		w.Flush()
		return
	}
	if strings.Contains(key, " ") {
		fmt.Fprintf(w, "ERR %s extra tokens\n", id)
		w.Flush()
		return
	}
	val, err := e.Get([]byte(key))
	if err != nil {
		if err == ErrNotFound {
			fmt.Fprintf(w, "NOTFOUND %s\n", id)
		} else {
			fmt.Fprintf(w, "ERR %s %s\n", id, err.Error())
		}
		w.Flush()
		return
	}
	fmt.Fprintf(w, "VAL %s %s\n", id, string(val))
	w.Flush()
}

func serveBatch(e *Engine, id, rest string, scanner *bufio.Scanner, w *bufio.Writer) {
	n, err := strconv.Atoi(strings.TrimSpace(rest))
	if err != nil || n <= 0 {
		fmt.Fprintf(w, "ERR %s invalid batch count\n", id)
		w.Flush()
		return
	}

	ops := make([]BatchOp, 0, n)
	parseError := false

	for i := 0; i < n; i++ {
		if !scanner.Scan() {
			// EOF mid-batch: apply nothing, send no reply.
			return
		}
		line := scanner.Text()
		op, err := parseBatchLine(line)
		if err != nil {
			parseError = true
			// Still consume all n lines.
			continue
		}
		if !parseError {
			ops = append(ops, op)
		}
	}

	if parseError {
		fmt.Fprintf(w, "ERR %s invalid batch sub-line\n", id)
		w.Flush()
		return
	}

	if err := e.Batch(ops); err != nil {
		fmt.Fprintf(w, "ERR %s %s\n", id, err.Error())
		w.Flush()
		return
	}
	fmt.Fprintf(w, "ACK %s\n", id)
	w.Flush()
}

func serveCompact(e *Engine, id, rest string, w *bufio.Writer) {
	if strings.TrimSpace(rest) != "" {
		fmt.Fprintf(w, "ERR %s extra tokens\n", id)
		w.Flush()
		return
	}
	if err := e.Compact(); err != nil {
		fmt.Fprintf(w, "ERR %s %s\n", id, err.Error())
		w.Flush()
		return
	}
	fmt.Fprintf(w, "ACK %s\n", id)
	w.Flush()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func parseBatchLine(line string) (BatchOp, error) {
	line = strings.TrimRight(line, "\r\n")
	if strings.HasPrefix(line, "PUT ") {
		rest := line[4:]
		idx := strings.IndexByte(rest, ' ')
		if idx < 0 || idx == 0 {
			return BatchOp{}, fmt.Errorf("invalid PUT line: %q", line)
		}
		key := rest[:idx]
		value := rest[idx+1:]
		return BatchOp{Key: []byte(key), Value: []byte(value)}, nil
	}
	if strings.HasPrefix(line, "DEL ") {
		key := strings.TrimSpace(line[4:])
		if key == "" || strings.Contains(key, " ") {
			return BatchOp{}, fmt.Errorf("invalid DEL line: %q", line)
		}
		return BatchOp{Key: []byte(key), Value: nil}, nil
	}
	return BatchOp{}, fmt.Errorf("invalid batch line: %q", line)
}
