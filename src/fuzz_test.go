package main

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestProtocolFuzz(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fuzz.db")
	eng, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = eng.Put([]byte("livekey"), []byte("liveval"))
	eng.Close()

	// 1MB+ value
	oneMBVal := strings.Repeat("M", 1024*1024+50)

	// Test inputs
	fuzzCases := []struct {
		name       string
		input      string
		expectResp string // prefix or substring
	}{
		{
			name:       "empty line",
			input:      "\n",
			expectResp: "ERR - empty line",
		},
		{
			name:       "only spaces",
			input:      "     \n",
			expectResp: "ERR - empty line",
		},
		{
			name:       "missing id",
			input:      "PUT\n",
			expectResp: "ERR - invalid command",
		},
		{
			name:       "missing op",
			input:      "req1\n",
			expectResp: "ERR - invalid command",
		},
		{
			name:       "PUT without value",
			input:      "req2 PUT onlykey\n",
			expectResp: "ERR req2 invalid PUT syntax",
		},
		{
			name:       "PUT without key or value",
			input:      "req3 PUT\n",
			expectResp: "ERR req3 invalid PUT syntax",
		},
		{
			name:       "DEL without key",
			input:      "req4 DEL\n",
			expectResp: "ERR req4 missing key",
		},
		{
			name:       "DEL with extra tokens",
			input:      "req5 DEL key1 extra\n",
			expectResp: "ERR req5 extra tokens",
		},
		{
			name:       "GET without key",
			input:      "req6 GET\n",
			expectResp: "ERR req6 missing key",
		},
		{
			name:       "GET with extra tokens",
			input:      "req7 GET key1 extra\n",
			expectResp: "ERR req7 extra tokens",
		},
		{
			name:       "unknown op",
			input:      "req8 FOOBAR some args\n",
			expectResp: "ERR req8 unknown command",
		},
		{
			name:       "COMPACT with extra tokens",
			input:      "req9 COMPACT now please\n",
			expectResp: "ERR req9 extra tokens",
		},
		{
			name:       "BATCH with bad count string",
			input:      "req10 BATCH abc\n",
			expectResp: "ERR req10 invalid batch count",
		},
		{
			name:       "BATCH with negative count",
			input:      "req11 BATCH -3\n",
			expectResp: "ERR req11 invalid batch count",
		},
		{
			name:       "BATCH with 0 count",
			input:      "req12 BATCH 0\n",
			expectResp: "ERR req12 invalid batch count",
		},
		{
			name:       "BATCH with bad sub-line (must still consume all n lines)",
			input:      "req13 BATCH 3\nPUT subk1 subval1\nMALFORMED SUB LINE\nPUT subk2 subval2\nreq14 GET livekey\n",
			expectResp: "ERR req13 invalid batch sub-line\nVAL req14 liveval",
		},
		{
			name:       "CRLF line endings",
			input:      "req15 PUT crlfkey crlfval\r\nreq16 GET crlfkey\r\n",
			expectResp: "ACK req15\nVAL req16 crlfval",
		},
		{
			name:       "NUL and binary bytes in value",
			input:      "req17 PUT binkey foo\x00bar\x01\x02\xffbaz\nreq18 GET binkey\n",
			expectResp: "ACK req17\nVAL req18 foo\x00bar\x01\x02\xffbaz",
		},
	}

	for _, tc := range fuzzCases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run([]string{"kvdb", dbPath, "serve"}, strings.NewReader(tc.input), &stdout, &stderr)
			if code != 0 {
				t.Fatalf("serve exited with %d for input %q: stderr=%s", code, tc.input, stderr.String())
			}

			out := strings.TrimSpace(stdout.String())
			if !strings.Contains(out, tc.expectResp) {
				t.Fatalf("expected response containing %q, got %q", tc.expectResp, out)
			}
		})
	}

	// Sub-test: EOF mid-batch: assert apply nothing, send no reply, process stays alive
	t.Run("EOF mid-batch", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		input := "reqE BATCH 3\nPUT eofk1 eofv1\n"
		code := run([]string{"kvdb", dbPath, "serve"}, strings.NewReader(input), &stdout, &stderr)
		if code != 0 {
			t.Fatalf("exit code %d: %s", code, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("expected no output on EOF mid batch, got %q", stdout.String())
		}
	})

	// Sub-test: 1MB+ line buffer overflow: prints ERR and exits non-zero
	t.Run("1MB+ line overflow", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		input := fmt.Sprintf("reqHuge PUT hugekey %s\n", oneMBVal)
		code := run([]string{"kvdb", dbPath, "serve"}, strings.NewReader(input), &stdout, &stderr)
		if code == 0 {
			t.Fatalf("expected non-zero exit on >1MB buffer overflow, got code 0")
		}
		if !strings.Contains(stdout.String(), "ERR") && !strings.Contains(stderr.String(), "scanner") {
			t.Fatalf("expected ERR on overflow, stdout=%q, stderr=%q", stdout.String(), stderr.String())
		}
	})

	// Sub-test: Stay alive and in sync after barrage of malformed lines
	t.Run("stay alive and in sync", func(t *testing.T) {
		barrage := strings.Join([]string{
			"",
			"bad1",
			"bad2 NOOP",
			"reqS1 PUT skey sval",
			"bad3 DEL",
			"reqS2 GET skey",
			"bad4 BATCH 2",
			"BAD LINE 1",
			"BAD LINE 2",
			"reqS3 GET skey",
		}, "\n") + "\n"

		var stdout, stderr bytes.Buffer
		code := run([]string{"kvdb", dbPath, "serve"}, strings.NewReader(barrage), &stdout, &stderr)
		if code != 0 {
			t.Fatalf("serve exited with %d: %s", code, stderr.String())
		}

		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		// Count responses
		expectedResponses := []string{
			"ERR - empty line",
			"ERR - invalid command",
			"ERR bad2 unknown command",
			"ACK reqS1",
			"ERR bad3 missing key",
			"VAL reqS2 sval",
			"ERR bad4 invalid batch sub-line",
			"VAL reqS3 sval",
		}

		if len(lines) != len(expectedResponses) {
			t.Fatalf("expected %d response lines, got %d:\n%v", len(expectedResponses), len(lines), lines)
		}
		for i, exp := range expectedResponses {
			if !strings.HasPrefix(lines[i], exp) {
				t.Errorf("line %d: expected prefix %q, got %q", i, exp, lines[i])
			}
		}
	})
}
