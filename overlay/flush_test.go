package overlay

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// flushOverlay opens an overlay that keeps each of names in memory with its
// own path as content, until Flush.
func flushOverlay(t *testing.T, names ...string) *FS {
	t.Helper()
	f, err := New(t.TempDir(), nil, nil, NewMemory(1<<20, 64<<10, 0))
	must(t, err)
	for _, p := range names {
		must(t, f.MkdirAll(filepath.Dir(p), 0o755))
		write(t, f, p, p, os.O_CREATE|os.O_WRONLY)
	}
	if !f.Unflushed() {
		t.Fatal("files are not in memory before Flush")
	}
	return f
}

// breakDir renames the upper directory dir away, so a spill below it fails.
func breakDir(t *testing.T, f *FS, dir string) {
	t.Helper()
	must(t, os.Rename(filepath.Join(f.upper, dir), filepath.Join(f.upper, dir+"2")))
}

// upperHolds fails unless the upper holds p with its path as content.
func upperHolds(t *testing.T, f *FS, p string) {
	t.Helper()
	if data, err := os.ReadFile(filepath.Join(f.upper, p)); err != nil || string(data) != p {
		t.Fatalf("upper %s holds %q, %v", p, data, err)
	}
}

// TestFlushContinuesAfterError spills the files after one that fails, keeps
// the failed one in memory, and names it in the error.
func TestFlushContinuesAfterError(t *testing.T) {
	f := flushOverlay(t, "a/x", "b/y")
	breakDir(t, f, "a")
	err := f.Flush()
	if err == nil || !strings.Contains(err.Error(), "flush a/x: ") || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Flush = %v, want a not-exist error naming a/x", err)
	}
	upperHolds(t, f, "b/y")
	if got := read(t, f, "a/x"); got != "a/x" {
		t.Fatalf("a/x reads %q after the failed spill", got)
	}
	if !f.Unflushed() {
		t.Fatal("Unflushed is false while a/x lives only in memory")
	}
}

// TestFlushJoinsEveryError reports every failed file, in sorted path order.
func TestFlushJoinsEveryError(t *testing.T) {
	f := flushOverlay(t, "c/z", "b/y", "a/x")
	breakDir(t, f, "a")
	breakDir(t, f, "c")
	err := f.Flush()
	if err == nil {
		t.Fatal("Flush = nil, want the errors of a/x and c/z")
	}
	msg := err.Error()
	a, c := strings.Index(msg, "flush a/x: "), strings.Index(msg, "flush c/z: ")
	if a < 0 || c < a || strings.Contains(msg, "b/y") {
		t.Fatalf("Flush = %q, want a/x then c/z and no b/y", msg)
	}
	upperHolds(t, f, "b/y")
}

// TestFlushSpillsEveryFile empties memory into the upper when nothing fails.
func TestFlushSpillsEveryFile(t *testing.T) {
	names := []string{"a/x", "b/y", "c/z"}
	f := flushOverlay(t, names...)
	must(t, f.Flush())
	for _, p := range names {
		upperHolds(t, f, p)
	}
	if f.Unflushed() {
		t.Fatal("Unflushed is true after a clean Flush")
	}
}
