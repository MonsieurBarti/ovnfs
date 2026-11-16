package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestCrashFileHoldsTheFatalTrace runs a child that records crashes and then
// panics: the crash file keeps the panic text and the goroutine trace.
func TestCrashFileHoldsTheFatalTrace(t *testing.T) {
	if state := os.Getenv("OVNFS_CRASH_TEST_STATE"); state != "" {
		if err := recordCrashes(state); err != nil {
			t.Fatal(err)
		}
		panic("probe crash")
	}
	state := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashFileHoldsTheFatalTrace$")
	cmd.Env = append(os.Environ(), "OVNFS_CRASH_TEST_STATE="+state)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("child exited cleanly after a panic:\n%s", out)
	}
	trace, err := os.ReadFile(crashPath(state))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"probe crash", "goroutine"} {
		if !strings.Contains(string(trace), want) {
			t.Fatalf("crash file lacks %q:\n%s", want, trace)
		}
	}
}
