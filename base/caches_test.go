package base

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/stencil-hq/ovnfs/internal/clone"
	"github.com/stencil-hq/ovnfs/internal/ident"
	"github.com/stencil-hq/ovnfs/overlay"
)

// sharesBlocks reports whether files in dir can be cloned; cache roots of a
// snapshot start empty where they cannot.
func sharesBlocks(t *testing.T, dir string) bool {
	t.Helper()
	probe := filepath.Join(dir, ".probe")
	writeTestFile(t, probe, "probe")
	defer os.Remove(probe)
	in, err := os.Open(probe)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	dst := filepath.Join(t.TempDir(), "clone")
	err = clone.Reflink(in, dst, 0o644)
	if errors.Is(err, errors.ErrUnsupported) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

// read returns the content of p in view.
func read(t *testing.T, view overlay.Bottom, p string) string {
	t.Helper()
	f, err := view.Open(p)
	if err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.NewSectionReader(f, 0, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// names lists directory p of view.
func names(t *testing.T, view overlay.Bottom, p string) []string {
	t.Helper()
	entries, err := view.ReadDir(p)
	if err != nil {
		t.Fatalf("readdir %s: %v", p, err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// TestCacheSnapshotIsolatesTheFolder serves cache and local roots as the
// snapshot holds them: files the folder writes, adds or deletes afterwards do
// not change the view, a later snapshot sees them while the earlier one keeps
// its content, and a symlink planted in the folder cannot lead the view
// outside it.
func TestCacheSnapshotIsolatesTheFolder(t *testing.T) {
	repo, dir := repository(t)
	if !sharesBlocks(t, dir) {
		t.Skip("the volume cannot clone: cache roots start empty")
	}
	writeTestFile(t, filepath.Join(dir, "node_modules", "pkg", "index.js"), "v1")
	writeTestFile(t, filepath.Join(dir, "node_modules", "gone", "a.js"), "gone")
	writeTestFile(t, filepath.Join(dir, ".env"), "SECRET=1")
	outside := t.TempDir()
	writeTestFile(t, filepath.Join(outside, "leak"), "outside")
	b, _ := createTestBase(t, repo, nil)
	first := b.TakeCaches("c1")
	first.Wait()
	old := b.Bottom(first)

	writeTestFile(t, filepath.Join(dir, "node_modules", "pkg", "index.js"), "v2")
	writeTestFile(t, filepath.Join(dir, "node_modules", "added", "x.js"), "added")
	writeTestFile(t, filepath.Join(dir, ".env"), "SECRET=2")
	writeTestFile(t, filepath.Join(dir, "new-source"), "new source")
	if err := os.RemoveAll(filepath.Join(dir, "node_modules", "gone")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "node_modules", "escape")); err != nil {
		t.Fatal(err)
	}

	for p, want := range map[string]string{"node_modules/pkg/index.js": "v1", "node_modules/gone/a.js": "gone", ".env": "SECRET=1"} {
		if got := read(t, old, p); got != want {
			t.Errorf("%s = %q after the folder changed, want the snapshot's %q", p, got, want)
		}
	}
	if _, err := old.Lstat("node_modules/added/x.js"); !os.IsNotExist(err) {
		t.Errorf("a file the folder added after the snapshot is visible: %v", err)
	}
	if got := names(t, old, "node_modules"); !slices.Equal(got, []string{"gone", "pkg"}) {
		t.Errorf("snapshot listing = %v", got)
	}
	if _, err := old.Lstat("new-source"); !os.IsNotExist(err) {
		t.Errorf("new source path visible: %v", err)
	}

	second := b.TakeCaches("c2")
	view := b.Bottom(second)
	for p, want := range map[string]string{"node_modules/pkg/index.js": "v2", "node_modules/added/x.js": "added", ".env": "SECRET=2"} {
		if got := read(t, view, p); got != want {
			t.Errorf("newer snapshot: %s = %q, want %q", p, got, want)
		}
	}
	if _, err := view.Lstat("node_modules/gone/a.js"); !os.IsNotExist(err) {
		t.Errorf("newer snapshot shows a deleted file: %v", err)
	}
	if _, err := view.Lstat("node_modules/escape/leak"); err == nil {
		t.Error("a symlink inside a cache root led outside the folder")
	}
	if got := read(t, old, "node_modules/pkg/index.js"); got != "v1" {
		t.Errorf("older snapshot changed with the newer one: %q", got)
	}
}

// TestCacheSnapshotFirstAccessWaits reads a cache root completely right after
// the snapshot starts: the access waits until the clone is complete instead
// of serving part of it.
func TestCacheSnapshotFirstAccessWaits(t *testing.T) {
	repo, dir := repository(t)
	if !sharesBlocks(t, dir) {
		t.Skip("the volume cannot clone: cache roots start empty")
	}
	const files = 2000
	for i := range files {
		writeTestFile(t, filepath.Join(dir, "target", "debug", fmt.Sprintf("f%04d", i)), "x")
	}
	b, _ := createTestBase(t, repo, nil)
	c := b.TakeCaches("c1")
	view := b.Bottom(c)
	view.(overlay.Awaiter).Await("target/debug")
	if got := names(t, view, "target/debug"); len(got) != files {
		t.Fatalf("first listing = %d entries, want %d", len(got), files)
	}
	view = b.Bottom(b.TakeCaches("c2"))
	if got := names(t, view, "target/debug"); len(got) != files {
		t.Fatalf("listing without Await = %d entries, want %d", len(got), files)
	}
}

// TestCacheSnapshotWithoutSharedBlocks starts cache roots empty and copies
// local roots where the state cannot share the folder's blocks.
func TestCacheSnapshotWithoutSharedBlocks(t *testing.T) {
	repo, dir := repository(t)
	writeTestFile(t, filepath.Join(dir, "node_modules", "pkg", "index.js"), "v1")
	writeTestFile(t, filepath.Join(dir, ".env"), "SECRET=1")
	b, _ := createTestBase(t, repo, nil)
	c := b.newCaches("c1")
	c.shared = false
	b.serve(c)
	c.take(b)
	view := b.Bottom(c)
	if got := names(t, view, "node_modules"); len(got) != 0 {
		t.Errorf("cache root = %v, want empty", got)
	}
	if got := read(t, view, ".env"); got != "SECRET=1" {
		t.Errorf("local root = %q, want a copy", got)
	}
}

// TestCacheSnapshotTakesQueueAndStop lists a snapshot's roots while every
// take slot is busy, and closes its base without waiting for the queued
// clone, which a later start takes again.
func TestCacheSnapshotTakesQueueAndStop(t *testing.T) {
	repo, dir := repository(t)
	writeTestFile(t, filepath.Join(dir, "node_modules", "pkg", "index.js"), "v1")
	b, config := createTestBase(t, repo, nil)
	for range cap(takeSlots) {
		takeSlots <- struct{}{}
	}
	t.Cleanup(func() {
		for range cap(takeSlots) {
			<-takeSlots
		}
	})
	c := b.TakeCaches("c1")
	select {
	case <-c.listed:
	case <-time.After(10 * time.Second):
		t.Fatal("roots were not listed while every take slot was busy")
	}
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close waited for a take queued behind busy slots")
	}
	if _, err := os.Stat(filepath.Join(config.StateDir, cachesDir, "c1", rootsFile)); !os.IsNotExist(err) {
		t.Errorf("a take stopped before its clone completed its snapshot: %v", err)
	}
}

// TestCacheSnapshotsReopenAndRemove keeps a complete snapshot across a
// restart, takes an incomplete one again, serves nothing once its base is
// closed and removes snapshots on request.
func TestCacheSnapshotsReopenAndRemove(t *testing.T) {
	repo, dir := repository(t)
	writeTestFile(t, filepath.Join(dir, ".env"), "SECRET=1")
	first, config := createTestBase(t, repo, nil)
	first.TakeCaches("c1").Wait()
	first.TakeCaches("c2").Wait()
	if got := read(t, first.Bottom(first.OpenCaches("c1")), ".env"); got != "SECRET=1" {
		t.Errorf("snapshot served again = %q", got)
	}
	writeTestFile(t, filepath.Join(dir, ".env"), "SECRET=2")
	if err := os.Remove(filepath.Join(config.StateDir, cachesDir, "c2", rootsFile)); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Bottom(first.OpenCaches("c1")).Lstat(".env"); !os.IsNotExist(err) {
		t.Errorf("a closed base's snapshot still serves: %v", err)
	}
	b, err := Open(config, first.Meta())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if got := read(t, b.Bottom(b.OpenCaches("c1")), ".env"); got != "SECRET=1" {
		t.Errorf("reopened snapshot = %q, want its own SECRET=1", got)
	}
	if got := read(t, b.Bottom(b.OpenCaches("c2")), ".env"); got != "SECRET=2" {
		t.Errorf("reopened incomplete snapshot = %q, want it taken again", got)
	}
	if err := b.RemoveCaches("c1"); err != nil {
		t.Fatal(err)
	}
	if ids, err := b.CachesIDs(); err != nil || !slices.Equal(ids, []string{"c2"}) {
		t.Fatalf("snapshots after removal = %v, %v", ids, err)
	}
}

// TestBaseIdentities gives captured entries their Capture identity, served
// from the folder or the capture alike, and snapshot files their State
// identity; Locate maps each back to its path.
func TestBaseIdentities(t *testing.T) {
	repo, dir := repository(t)
	writeTestFile(t, filepath.Join(dir, "node_modules", "pkg", "index.js"), "v1")
	b, _ := createTestBase(t, repo, nil)
	c := b.TakeCaches("c1")
	c.Wait()
	view := b.Bottom(c).(bottom)
	writeTestFile(t, filepath.Join(dir, "source.txt"), "changed, served from the capture")
	for _, p := range []string{"", "src", "src/nested.txt", "source.txt", "node_modules"} {
		info, err := view.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		id, ok := ident.Of(info)
		n, _ := b.index.lookup(p)
		if !ok || id != captureID(n) {
			t.Errorf("%q: identity %v, want Capture %d", p, id, n)
		}
		if got, ok := view.Locate(id); !ok || got != p {
			t.Errorf("locate %v = %q, %v; want %q", id, got, ok, p)
		}
	}
	if sharesBlocks(t, dir) { // else the cache root starts empty
		info, err := view.Lstat("node_modules/pkg/index.js")
		if err != nil {
			t.Fatal(err)
		}
		id, ok := ident.Of(info)
		if !ok || id.Space() != ident.State {
			t.Fatalf("snapshot file identity = %v, %v; want State", id, ok)
		}
		if runtime.GOOS == "darwin" {
			if got, ok := view.Locate(id); !ok || got != "node_modules/pkg/index.js" {
				t.Errorf("locate snapshot file = %q, %v", got, ok)
			}
		}
	}
	hidden, _ := ident.New(ident.Capture, uint64(b.index.nodes))
	if _, ok := view.Locate(hidden); ok {
		t.Error("an identity beyond the capture was located")
	}
}

// TestSnapshotLinksIntoTheFolderAreRelative serves a snapshot link's absolute
// target inside the folder relative to the link, so a checkout's link names
// the checkout's copy; targets outside the folder and relative ones stay as
// stored.
func TestSnapshotLinksIntoTheFolderAreRelative(t *testing.T) {
	repo, dir := repository(t)
	writeTestFile(t, filepath.Join(dir, "node_modules", "pkg", "index.js"), "v1")
	b, _ := createTestBase(t, repo, nil)
	outside := t.TempDir()
	for link, target := range map[string]string{
		"node_modules/@x/src":  filepath.Join(b.Meta().Dir, "src"),
		"node_modules/root":    b.Meta().Dir,
		"node_modules/escape":  outside,
		"node_modules/sibling": "pkg",
	} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, link)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, filepath.FromSlash(link))); err != nil {
			t.Fatal(err)
		}
	}
	if !sharesBlocks(t, dir) {
		t.Skip("the volume cannot clone: cache roots start empty")
	}
	view := b.Bottom(b.TakeCaches("c1"))
	for link, want := range map[string]string{
		"node_modules/@x/src":  "../../src",
		"node_modules/root":    "..",
		"node_modules/escape":  filepath.ToSlash(outside),
		"node_modules/sibling": "pkg",
	} {
		if got, err := view.Readlink(link); err != nil || got != want {
			t.Errorf("readlink %s = %q, %v; want %q", link, got, err, want)
		}
	}
}

// TestCachesAffectedBy marks a snapshot stale only for changes at, below or
// above a cache or local root.
func TestCachesAffectedBy(t *testing.T) {
	repo, dir := repository(t)
	writeTestFile(t, filepath.Join(dir, "a", "node_modules", "x.js"), "x")
	b, _ := createTestBase(t, repo, nil)
	for p, want := range map[string]bool{"": true, "a": true, "a/node_modules": true, "a/node_modules/x.js": true, "source.txt": false, "src/nested.txt": false, "a/b": false} {
		if got := b.CachesAffectedBy(p); got != want {
			t.Errorf("CachesAffectedBy(%q) = %v, want %v", p, got, want)
		}
	}
}

// BenchmarkCacheSnapshotLstat measures Lstat of one entry below a cache root,
// over a 30,000-entry snapshot directory, as NFS LOOKUP and GETATTR issue it.
func BenchmarkCacheSnapshotLstat(b *testing.B) {
	repo, dir := repository(b)
	const entries = 30000
	names := make([]string, entries)
	for i := range names {
		names[i] = fmt.Sprintf("target/debug/deps/f%05d", i)
		writeTestFile(b, filepath.Join(dir, filepath.FromSlash(names[i])), "x")
	}
	base, _ := createTestBase(b, repo, nil)
	c := base.TakeCaches("c1")
	c.Wait()
	view := base.Bottom(c)
	for _, p := range names {
		if _, err := view.Lstat(p); err != nil {
			b.Fatal(err)
		}
	}
	i := 0
	for b.Loop() {
		if _, err := view.Lstat(names[i%entries]); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
