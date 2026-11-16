package overlay

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// waitUntil polls cond for up to 5s.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: still false after 5s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// memOverlay opens an overlay on mem whose files (d/new by default) hold
// "hello" in memory.
func memOverlay(t *testing.T, mem *Memory, names ...string) *FS {
	t.Helper()
	f, err := New(t.TempDir(), nil, nil, mem)
	must(t, err)
	must(t, f.MkdirAll("d", 0o755))
	if len(names) == 0 {
		names = []string{"d/new"}
	}
	for _, p := range names {
		write(t, f, p, "hello", os.O_CREATE|os.O_WRONLY)
	}
	return f
}

// park parks every memory file of f with its work directory gone, so every
// retry fails until the test recreates f.work.
func park(t *testing.T, f *FS) {
	t.Helper()
	must(t, os.RemoveAll(f.work))
	f.mu.Lock()
	for _, m := range f.files {
		f.mem.park(m, errors.New("no space left on device"))
	}
	f.mu.Unlock()
}

// parkedOverlay is a memOverlay whose d/new is parked.
func parkedOverlay(t *testing.T, mem *Memory) *FS {
	t.Helper()
	f := memOverlay(t, mem)
	park(t, f)
	return f
}

// TestParkedFileRetriesWriteback writes back a file whose idle spill failed
// once the work directory returns, instead of keeping it in memory for good.
func TestParkedFileRetriesWriteback(t *testing.T) {
	f, err := New(t.TempDir(), nil, nil, NewMemory(1<<20, 64<<10, 20*time.Millisecond))
	must(t, err)
	must(t, f.MkdirAll("d", 0o755))
	must(t, os.RemoveAll(f.work))
	write(t, f, "d/new", "hello", os.O_CREATE|os.O_WRONLY)
	time.Sleep(100 * time.Millisecond)
	must(t, os.Mkdir(f.work, 0o700))
	waitUntil(t, "parked file written back", func() bool { return !f.Unflushed() })
	if data, err := os.ReadFile(filepath.Join(f.upper, "d/new")); err != nil || string(data) != "hello" {
		t.Fatalf("upper holds %q, %v", data, err)
	}
}

// TestAdmitRefusesNewFilesWhileParked sends new files to disk while any file
// waits in parked, and back to memory once the retry succeeds.
func TestAdmitRefusesNewFilesWhileParked(t *testing.T) {
	mem := NewMemory(1<<20, 64<<10, 0)
	mem.retryEvery = 5 * time.Millisecond
	f := parkedOverlay(t, mem)
	before, _ := mem.Usage()
	write(t, f, "other", "x", os.O_CREATE|os.O_WRONLY)
	if data, err := os.ReadFile(filepath.Join(f.upper, "other")); err != nil || string(data) != "x" {
		t.Fatalf("new file did not land in the upper: %q, %v", data, err)
	}
	if after, _ := mem.Usage(); after != before {
		t.Fatalf("memory grew from %d to %d while a file was parked", before, after)
	}
	must(t, os.Mkdir(f.work, 0o700))
	waitUntil(t, "parked list empty", func() bool { n, _ := mem.Parked(); return n == 0 })
	write(t, f, "third", "y", os.O_CREATE|os.O_WRONLY)
	if !f.Unflushed() {
		t.Fatal("a new file after recovery did not stay in memory")
	}
	if _, err := os.Lstat(filepath.Join(f.upper, "third")); err == nil {
		t.Fatal("a new file after recovery reached the upper")
	}
}

// TestMemoryReportsParkedFiles reports the parked count and the latest error
// while a file waits, and nothing once its retry succeeds.
func TestMemoryReportsParkedFiles(t *testing.T) {
	mem := NewMemory(1<<20, 64<<10, 0)
	mem.retryEvery = 5 * time.Millisecond
	f := parkedOverlay(t, mem)
	if n, msg := mem.Parked(); n != 1 || msg == "" {
		t.Fatalf("Parked() = %d, %q while a file waits", n, msg)
	}
	must(t, os.Mkdir(f.work, 0o700))
	waitUntil(t, "parked list empty", func() bool { n, _ := mem.Parked(); return n == 0 })
	if n, msg := mem.Parked(); n != 0 || msg != "" {
		t.Fatalf("Parked() = %d, %q after recovery", n, msg)
	}
}

// TestParkLogsFirstFailurePerOverlay logs one line per overlay however many
// of its files park and however often their retries fail.
func TestParkLogsFirstFailurePerOverlay(t *testing.T) {
	mem := NewMemory(1<<20, 64<<10, 0)
	mem.retryEvery = 5 * time.Millisecond
	var mu sync.Mutex
	var lines []string
	mem.Logf = func(format string, args ...any) {
		mu.Lock()
		lines = append(lines, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	a := memOverlay(t, mem, "d/new", "d/second")
	b := memOverlay(t, mem)
	park(t, a)
	park(t, b)
	time.Sleep(50 * time.Millisecond) // several failed retries
	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 2 {
		t.Fatalf("logged %d lines, want one per overlay: %q", len(lines), lines)
	}
	for i, f := range []*FS{a, b} {
		if !strings.Contains(lines[i], f.upper) || !strings.Contains(lines[i], "no space left on device") {
			t.Fatalf("line %q lacks the upper %s or the error", lines[i], f.upper)
		}
	}
}

// TestParkedFileReleasedOnUnlink takes a parked file off the list and returns
// its charge when it is unlinked, discarded or flushed.
func TestParkedFileReleasedOnUnlink(t *testing.T) {
	for name, release := range map[string]func(*FS) error{
		"unlink":  func(f *FS) error { return f.Remove("d/new") },
		"discard": func(f *FS) error { f.Discard(); return nil },
		"flush": func(f *FS) error {
			if err := os.Mkdir(f.work, 0o700); err != nil {
				return err
			}
			return f.Flush()
		},
	} {
		t.Run(name, func(t *testing.T) {
			mem := NewMemory(1<<20, 64<<10, 0)
			mem.retryEvery = time.Hour // only the case under test may clear the file
			start, _ := mem.Usage()
			f := parkedOverlay(t, mem)
			must(t, release(f))
			if n, _ := mem.Parked(); n != 0 {
				t.Fatalf("%d files still parked", n)
			}
			if used, _ := mem.Usage(); used != start {
				t.Fatalf("%d bytes charged, want %d", used, start)
			}
		})
	}
}

// TestParkIgnoresRetiredFile leaves out a file that already left the
// namespace, so a retry never chases a file that has no memory data.
func TestParkIgnoresRetiredFile(t *testing.T) {
	mem := NewMemory(1<<20, 64<<10, 0)
	mem.retryEvery = time.Hour
	f, err := New(t.TempDir(), nil, nil, mem)
	must(t, err)
	write(t, f, "gone", "data", os.O_CREATE|os.O_WRONLY)
	f.mu.Lock()
	m := f.files["gone"]
	f.detach(m)
	mem.park(m, errors.New("no space left on device"))
	f.mu.Unlock()
	if n, msg := mem.Parked(); n != 0 || msg != "" {
		t.Fatalf("Parked() = %d, %q for a retired file", n, msg)
	}
}
