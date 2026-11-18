package base

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stencil-hq/ovnfs/internal/clone"
	"github.com/stencil-hq/ovnfs/internal/hostfile"
	"github.com/stencil-hq/ovnfs/internal/search/fsys"
	"github.com/stencil-hq/ovnfs/rules"
)

// A checkout of a local base never reads the folder's cache and local roots
// (build output, secrets, large files) as they are now: it reads a snapshot of
// them that ovnfs owns, so host builds writing the folder change no checkout
// and no folder event invalidates one. A snapshot clones the roots' whole
// hierarchies below the base's state (cachesDir/ID/treeDir/ROOT), sharing
// their blocks where the volume can (APFS clonefile(2), FICLONE, block
// cloning on ReFS). It is taken in the background: checkout creation never
// waits for it, the first access below a root waits until it is complete.
//
// Where blocks cannot be shared (the state on another volume than the folder,
// a filesystem without cloning) or the clone fails, a cache root starts empty,
// as on remote bases, and is rebuilt in the checkout; a local root is copied,
// and starts empty only when even that fails (reported as a fault).
//
// A snapshot is complete once rootsFile is written. A daemon stopped before
// that retakes it on the next start (OpenCaches); nothing below its roots was
// served yet.
const (
	cachesDir = "caches"
	treeDir   = "tree"
	rootsFile = "roots.json"
)

// takeSlots bounds the snapshots that clone directory roots at once, across
// every base of the daemon.
var takeSlots = make(chan struct{}, 2)

// Caches is one copy-on-write snapshot of a local base's cache and local
// roots, which the checkouts the registry opens on it read instead of the
// folder (see [Base.Bottom]). The registry decides which checkouts share one:
// every checkout opened while the folder did not change below those roots.
type Caches struct {
	id    string
	dir   string // the snapshot's directory below the base's state
	tree  string // dir's treeDir, the root-relative image of the folder's roots
	files *tree  // tree, resolving lookups below it
	// listed is closed once roots is set: every root is known, and roots that
	// are no directory are already in tree.
	listed chan struct{}
	// done is closed once every root is in tree; ready then reads true.
	done  chan struct{}
	ready atomic.Bool
	roots map[string]cacheRoot // the roots the snapshot holds; written before listed closes
	// proofs holds directories below a root proven real (no symlink) in
	// tree. The snapshot never changes, so a proof never expires.
	proofsMu sync.Mutex
	proofs   map[string]bool
	locator  locator // resolves State identities to tree paths (platform)
	shared   bool    // take: the folder and the snapshot share a volume
}

// cacheRoot is a root the snapshot holds, as the folder had it when the
// snapshot began. A directory root presents this metadata (see [Caches.lstat]).
type cacheRoot struct {
	Mode  os.FileMode `json:"mode"`
	Mtime time.Time   `json:"mtime"`
}

// ID returns the snapshot's name below the base's state, as the registry persists it.
func (c *Caches) ID() string { return c.id }

// Wait blocks until the snapshot is complete. The registry's tests use it;
// views wait by themselves (see [bottom.Await]).
func (c *Caches) Wait() { <-c.done }

// newCaches describes snapshot id of b, not yet listed or complete.
func (b *Base) newCaches(id string) *Caches {
	dir := filepath.Join(b.config.StateDir, cachesDir, id)
	t := filepath.Join(dir, treeDir)
	return &Caches{id: id, dir: dir, tree: t, files: newTree(t), listed: make(chan struct{}), done: make(chan struct{})}
}

// TakeCaches starts snapshot id of a local base's cache and local roots in the
// background and returns it at once; any earlier content of id is replaced.
// The registry calls it when it opens a checkout and the newest snapshot is
// stale or missing.
func (b *Base) TakeCaches(id string) *Caches {
	c := b.newCaches(id)
	c.shared = sameVolume(b.meta.Dir, b.config.StateDir)
	b.serve(c)
	go c.take(b)
	return c
}

// OpenCaches returns snapshot id: the one already served, else the one on
// disk, reopened after a restart. Complete, it serves at once; otherwise it is
// taken anew (see [Base.TakeCaches]).
func (b *Base) OpenCaches(id string) *Caches {
	b.mu.Lock()
	c := b.caches[id]
	b.mu.Unlock()
	if c != nil {
		return c
	}
	c = b.newCaches(id)
	data, err := os.ReadFile(filepath.Join(c.dir, rootsFile))
	if err == nil {
		err = json.Unmarshal(data, &c.roots)
	}
	if err == nil {
		err = c.files.open()
	}
	if err != nil {
		return b.TakeCaches(id)
	}
	close(c.listed)
	c.ready.Store(true)
	close(c.done)
	b.serve(c)
	return c
}

// serve records c as served, so removing it or closing the base releases it.
func (b *Base) serve(c *Caches) {
	b.mu.Lock()
	b.caches[c.id] = c
	b.mu.Unlock()
}

// CachesIDs lists the snapshots on disk, complete or not, so the registry can
// remove those no checkout names.
func (b *Base) CachesIDs() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(b.config.StateDir, cachesDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.Name())
	}
	return ids, nil
}

// RemoveCaches deletes snapshot id, once a take in progress has ended, and
// releases its open tree. The registry calls it for snapshots no checkout
// uses any more.
func (b *Base) RemoveCaches(id string) error {
	b.mu.Lock()
	c := b.caches[id]
	delete(b.caches, id)
	b.mu.Unlock()
	if c != nil {
		c.Wait()
		c.files.close()
	}
	return os.RemoveAll(filepath.Join(b.config.StateDir, cachesDir, id))
}

// served lists the snapshots served.
func (b *Base) served() []*Caches {
	b.mu.Lock()
	defer b.mu.Unlock()
	served := make([]*Caches, 0, len(b.caches))
	for _, c := range b.caches {
		served = append(served, c)
	}
	return served
}

// take copies b's cache and local roots into the snapshot: first the roots
// that are no directory, quickly, so listings can present every root, then
// the directory roots. A root is taken while every ancestor in the folder is
// a real directory, so a symlink in the folder cannot pull outside content in.
func (c *Caches) take(b *Base) {
	roots := make(map[string]cacheRoot)
	defer func() {
		c.ready.Store(true)
		close(c.done)
	}()
	listed := sync.OnceFunc(func() {
		c.roots = roots
		close(c.listed)
	})
	defer listed()
	// A leftover of an earlier take (a daemon stopped mid-clone) moves aside
	// at once: removing a cloned node_modules takes minutes, and listings of
	// the checkout wait for listed. A stop before its removal leaves it to
	// the registry, which removes snapshots no checkout names.
	old := c.dir + ".old" + strconv.FormatInt(time.Now().UnixNano(), 36)
	var cleared error
	if err := os.Rename(c.dir, old); err != nil {
		old = ""
		if !errors.Is(err, os.ErrNotExist) {
			cleared = os.RemoveAll(c.dir)
		}
	}
	if err := errors.Join(cleared, os.MkdirAll(c.tree, 0o700), c.files.open()); err != nil {
		b.fault(fmt.Sprintf("cache snapshot %s: %v; cache and local roots are empty", c.id, err))
		return
	}
	var dirs []string
	for _, p := range b.cacheRoots {
		info, ok := b.folderRoot(p)
		if !ok {
			continue
		}
		if info.IsDir() {
			roots[p] = cacheRoot{Mode: info.Mode(), Mtime: info.ModTime()}
			dirs = append(dirs, p)
		} else if c.copyRoot(b, p, info) {
			roots[p] = cacheRoot{Mode: info.Mode(), Mtime: info.ModTime()}
		}
	}
	listed()
	// Directory roots clone one snapshot at a time per slot: a start that
	// retakes every checkout's snapshot at once thrashes the volume for
	// minutes, and a stop waits for each clone in progress.
	select {
	case takeSlots <- struct{}{}:
		defer func() { <-takeSlots }()
	case <-b.closing:
		return
	}
	if old != "" {
		os.RemoveAll(old)
	}
	for _, p := range dirs {
		select {
		case <-b.closing:
			return // without rootsFile: the next start takes it again
		default:
		}
		c.copyRoot(b, p, roots[p])
	}
	data, err := json.Marshal(roots)
	if err == nil {
		tmp := filepath.Join(c.dir, rootsFile+".tmp")
		if err = os.WriteFile(tmp, data, 0o600); err == nil {
			err = os.Rename(tmp, filepath.Join(c.dir, rootsFile))
		}
	}
	if err != nil {
		b.fault(fmt.Sprintf("cache snapshot %s: %v; it is taken again at the next start", c.id, err))
	}
}

// dirKind is what copyRoot needs of a root's metadata: whether it is a directory.
type dirKind interface{ IsDir() bool }

// IsDir reports a directory root.
func (r cacheRoot) IsDir() bool { return r.Mode.IsDir() }

// copyRoot copies root p into the snapshot and reports whether it is there:
// cloned (c.shared), or copied for a local root. A failed
// directory root stays as an empty directory; a failed file is left out.
func (c *Caches) copyRoot(b *Base, p string, kind dirKind) bool {
	src, dst := b.lower(p), c.path(p)
	err := os.MkdirAll(filepath.Dir(dst), 0o700)
	if err == nil {
		switch class, _ := b.Class(p); {
		case class == rules.Local:
			err = clone.Tree(src, dst)
		case c.shared:
			err = clone.Shared(src, dst)
		default:
			err = errors.ErrUnsupported
		}
	}
	if err == nil {
		return true
	}
	os.RemoveAll(dst)
	if !errors.Is(err, errors.ErrUnsupported) {
		b.fault(fmt.Sprintf("cache snapshot of %s: %v; it starts empty", p, err))
	}
	return kind.IsDir() && os.Mkdir(dst, 0o755) == nil
}

// folderRoot returns the folder's entry at root p while every ancestor of p
// in the folder is a real directory.
func (b *Base) folderRoot(p string) (os.FileInfo, bool) {
	for dir := path.Dir(p); dir != "."; dir = path.Dir(dir) {
		if info, err := os.Lstat(b.lower(dir)); err != nil || !info.IsDir() {
			return nil, false
		}
	}
	info, err := hostfile.Lstat(b.lower(p))
	return info, err == nil
}

// path names p's entry in the snapshot's tree.
func (c *Caches) path(p string) string { return filepath.Join(c.tree, filepath.FromSlash(p)) }

// lstat serves p, at or below the snapshot's root (index node n), as the
// snapshot holds it. A directory root
// presents the folder's metadata of when the snapshot began and its Capture
// identity, so listing its parent never waits for the clone; any other path
// waits until the snapshot is complete and carries its own (State) identity.
func (c *Caches) lstat(p, root string, n uint32) (os.FileInfo, error) {
	<-c.listed
	r, ok := c.roots[root]
	if !ok {
		return nil, absent(p)
	}
	if p == root && r.Mode.IsDir() {
		return capturedInfo{name: path.Base(p), mode: r.Mode, mtime: r.Mtime, id: captureID(n)}, nil
	}
	<-c.done
	if p != root && (!r.Mode.IsDir() || !c.dirsBelow(root, p)) {
		return nil, absent(p)
	}
	return c.files.lstat(p)
}

// list returns the entries of snapshot directory p, once complete.
func (c *Caches) list(p string) ([]os.FileInfo, error) {
	<-c.done
	dir := c.path(p)
	entries, err := fsys.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]os.FileInfo, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		host := filepath.Join(dir, e.Name())
		out = append(out, stateInfo(hostfile.Linked(info, host), host))
	}
	return out, nil
}

// dirsBelow reports whether every directory from below root down to p's
// parent is a real directory in the snapshot.
func (c *Caches) dirsBelow(root, p string) bool {
	var unproven []string
	c.proofsMu.Lock()
	for dir := path.Dir(p); dir != root; dir = path.Dir(dir) {
		if c.proofs[dir] {
			break
		}
		unproven = append(unproven, dir)
	}
	c.proofsMu.Unlock()
	for _, dir := range unproven {
		if info, err := c.files.lstat(dir); err != nil || !info.IsDir() {
			return false
		}
	}
	if len(unproven) == 0 {
		return true
	}
	c.proofsMu.Lock()
	defer c.proofsMu.Unlock()
	if c.proofs == nil || len(c.proofs) >= 1<<16 {
		c.proofs = make(map[string]bool)
	}
	for _, dir := range unproven {
		c.proofs[dir] = true
	}
	return true
}

// await blocks until p can be served without waiting: the roots are known
// and, at or below a root, the snapshot is complete.
func (c *Caches) await(b *Base, p string) {
	<-c.listed
	if _, root, class := b.find(p); root != "" && snapshotted(class) {
		<-c.done
	}
}

// snapshotted reports a class of roots a cache snapshot serves.
func snapshotted(class rules.Class) bool { return class == rules.Cache || class == rules.Local }

// cacheRootsOf lists, sorted, the cache and local roots of capture index x.
func cacheRootsOf(x *captureIndex) []string {
	var roots []string
	x.eachRoot(func(p string, class rules.Class) {
		if snapshotted(class) {
			roots = append(roots, p)
		}
	})
	slices.Sort(roots)
	return roots
}

// CachesAffectedBy reports whether a folder change at p can make a cache
// snapshot taken before it differ from the folder: p is at or below a cache or
// local root, or above one, or "" (the whole folder, when events were lost).
// The registry's watcher marks the newest snapshot stale with it.
func (b *Base) CachesAffectedBy(p string) bool {
	if p == "" {
		return len(b.cacheRoots) != 0
	}
	if _, root, class := b.find(p); root != "" && snapshotted(class) {
		return true
	}
	prefix := p + "/"
	i, _ := slices.BinarySearch(b.cacheRoots, prefix)
	return i < len(b.cacheRoots) && len(b.cacheRoots[i]) > len(prefix) && b.cacheRoots[i][:len(prefix)] == prefix
}

// fault reports a problem through the configured sink.
func (b *Base) fault(msg string) {
	if b.config.OnFault != nil {
		b.config.OnFault(fmt.Sprintf("base %s: %s", b.meta.ID, msg))
	}
}

// sameVolume reports whether a and b are on one volume, where blocks can be shared.
func sameVolume(a, b string) bool {
	va, errA := volumeID(a)
	vb, errB := volumeID(b)
	return errA == nil && errB == nil && va == vb
}
