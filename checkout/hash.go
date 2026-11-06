package checkout

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/stencil-hq/ovnfs/overlay"
	"github.com/stencil-hq/ovnfs/rules"
	"github.com/stencil-hq/ovnfs/source"
	"github.com/stencil-hq/ovnfs/store"
)

// hashJob describes an immutable snapshot and the ancestor tree it extends.
type hashJob struct {
	Lineage  string
	Snapshot string
	Base     *baseRecord
	Parent   store.OID
	Layers   []*overlay.Layer // oldest first
	View     *overlay.FS      // the snapshot's frozen view, read for rule facts
	Done     func(tree store.OID, err error)
}

// hasher serializes each lineage without holding up independent lineages.
type hasher struct {
	mu      sync.Mutex
	queues  map[string][]hashJob
	closed  bool
	workers sync.WaitGroup
}

// newHasher creates an idle queue; workers exist only while jobs are pending.
func newHasher() *hasher {
	return &hasher{queues: make(map[string][]hashJob)}
}

// queue appends a snapshot in FIFO order, starting its lineage when idle.
func (h *hasher) queue(j hashJob) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		if j.Done != nil {
			j.Done("", errors.New("snapshot hasher is closed"))
		}
		return
	}
	_, running := h.queues[j.Lineage]
	h.queues[j.Lineage] = append(h.queues[j.Lineage], j)
	if !running {
		h.workers.Add(1)
		go h.run(j.Lineage)
	}
	h.mu.Unlock()
}

// run owns one lineage until its queue is empty, including completion callbacks.
func (h *hasher) run(lineage string) {
	defer h.workers.Done()
	for {
		h.mu.Lock()
		jobs := h.queues[lineage]
		if len(jobs) == 0 {
			delete(h.queues, lineage)
			h.mu.Unlock()
			return
		}
		j := jobs[0]
		jobs[0] = hashJob{}
		h.queues[lineage] = jobs[1:]
		h.mu.Unlock()

		id, err := hashSnapshot(j)
		if j.Done != nil {
			j.Done(id, err)
		}
	}
}

// close prevents new jobs and waits for all accepted jobs and callbacks.
func (h *hasher) close() {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	h.workers.Wait()
}

// hashSnapshot stores the snapshot's source-class changes and writes its tree.
func hashSnapshot(j hashJob) (store.OID, error) {
	st := j.Base.view.Store()
	t, err := newTree(j.Base, st, j.Parent, overlayView{j.View})
	if err != nil {
		return "", err
	}
	for _, l := range j.Layers {
		if err := t.apply(l); err != nil {
			return "", err
		}
	}
	return st.Build(t.finish())
}

// overlayView reads rule facts through an overlay.
type overlayView struct{ fs *overlay.FS }

// Lstat returns merged metadata without following a final symlink.
func (v overlayView) Lstat(p string) (os.FileInfo, error) { return v.fs.Lstat(p) }

// ReadFile reads a merged regular file.
func (v overlayView) ReadFile(p string) ([]byte, error) {
	f, err := v.fs.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// tree applies layers to a parent tree: source-class files are hashed (and stored
// unless store is nil), every other class is left out, and paths the parent
// already held stay source whatever the rules now say.
type tree struct {
	base   *baseRecord
	store  *store.Store
	view   source.View
	files  map[string]store.File
	sorted []string          // parent paths, for prefix removal without full scans
	added  map[string]bool   // paths set by layers
	hosts  map[string]string // path → host file whose content a layer supplied
	dirs   map[string]bool   // source directories layers created, kept when empty
	class  map[string]rules.Class
}

// newTree starts from a flattened parent tree.
func newTree(b *baseRecord, st *store.Store, parent store.OID, view source.View) (*tree, error) {
	files, err := b.view.Store().Flatten(parent)
	if err != nil {
		return nil, err
	}
	return &tree{base: b, store: st, view: view, files: files, sorted: source.SortedPaths(files), added: map[string]bool{}, hosts: map[string]string{}, dirs: map[string]bool{}, class: map[string]rules.Class{"": rules.Source}}, nil
}

// apply folds one layer's entries, visited parents first, into the tree.
func (t *tree) apply(l *overlay.Layer) error {
	return l.Walk(func(p string, e overlay.Entry) error {
		if p == "" || hashExcluded(p) {
			return nil
		}
		// A non-source directory is one unit: nothing below it is classified or hashed.
		if pc := parentClass(t.class, p); pc != rules.Source {
			if e.Kind == overlay.KindDir {
				t.class[p] = pc
			}
			return nil
		}
		switch e.Kind {
		case overlay.KindWhiteout:
			t.remove(p)
		case overlay.KindDir:
			if e.Opaque {
				t.removeUnder(p)
			}
			class := t.classify(p, e.Info)
			t.class[p] = class
			if f, ok := t.files[p]; ok && f.Mode != store.ModeDir {
				delete(t.files, p)
			}
			if class != rules.Source {
				t.remove(p)
				return nil
			}
			t.dirs[p] = true
		case overlay.KindFile, overlay.KindSymlink:
			if t.classify(p, e.Info) != rules.Source {
				return nil
			}
			t.removeUnder(p)
			delete(t.dirs, p)
			host := filepath.Join(l.Dir(), filepath.FromSlash(p))
			f, err := t.hash(host, e)
			if err != nil {
				return err
			}
			t.files[p], t.added[p], t.hosts[p] = f, true, host
		}
		return nil
	})
}

// parentClass returns the class decided for p's parent directory (the root is source).
func parentClass(classes map[string]rules.Class, p string) rules.Class {
	parent := path.Dir(p)
	if parent == "." {
		parent = ""
	}
	if c, ok := classes[parent]; ok {
		return c
	}
	return rules.Source
}

// classify decides a layer path whose parents are source: a path the tree already
// holds stays source, a base root keeps the class the capture gave it, and
// anything new is decided by the base's rules.
func (t *tree) classify(p string, info os.FileInfo) rules.Class {
	if _, ok := t.files[p]; ok {
		return rules.Source
	}
	if class, ok := t.base.view.Capture().Roots[p]; ok {
		return class
	}
	if rule, ok := t.base.rules.Match(p, t.base.view.Domains().Facts(t.view, p, info)); ok {
		return rule.Class
	}
	return rules.Source
}

// hash computes (and, with a store, keeps) the blob of one layer file or symlink.
func (t *tree) hash(host string, e overlay.Entry) (store.File, error) {
	mode, ok := store.ModeOf(e.Info.Mode())
	if !ok {
		return store.File{}, &os.PathError{Op: "hash", Path: host, Err: os.ErrInvalid}
	}
	if mode == store.ModeSymlink {
		target, err := os.Readlink(host)
		if err != nil {
			return store.File{}, err
		}
		target = filepath.ToSlash(target) // Windows returns backslashes; snapshots are portable
		if t.store == nil {
			return store.File{Mode: mode, ID: store.HashBytes("blob", []byte(target))}, nil
		}
		id, err := t.store.PutBytes([]byte(target))
		return store.File{Mode: mode, ID: id}, err
	}
	var (
		id  store.OID
		err error
	)
	if t.store == nil {
		id, _, err = store.HashFile(host)
	} else {
		id, _, err = t.store.PutFile(host)
	}
	return store.File{Mode: mode, ID: id}, err
}

// remove drops p and everything below it.
func (t *tree) remove(p string) {
	delete(t.files, p)
	delete(t.added, p)
	delete(t.hosts, p)
	delete(t.dirs, p)
	t.removeUnder(p)
}

// removeUnder drops everything strictly below p.
func (t *tree) removeUnder(p string) {
	prefix := p + "/"
	for i := sort.SearchStrings(t.sorted, prefix); i < len(t.sorted) && strings.HasPrefix(t.sorted[i], prefix); i++ {
		delete(t.files, t.sorted[i])
	}
	for q := range t.added {
		if strings.HasPrefix(q, prefix) {
			delete(t.files, q)
			delete(t.added, q)
			delete(t.hosts, q)
		}
	}
	for d := range t.dirs {
		if strings.HasPrefix(d, prefix) {
			delete(t.dirs, d)
		}
	}
}

// finish turns created directories left without leaves into empty-directory
// leaves and drops empty-directory leaves that gained children. A created
// directory is a child too, so nested empty directories keep only the deepest.
func (t *tree) finish() map[string]store.File {
	parents := make(map[string]bool)
	markParents := func(p string) {
		for d := path.Dir(p); d != "."; d = path.Dir(d) {
			if parents[d] {
				break
			}
			parents[d] = true
		}
	}
	for p := range t.files {
		markParents(p)
	}
	for d := range t.dirs {
		markParents(d)
	}
	for d := range t.dirs {
		if !parents[d] {
			t.files[d] = store.File{Mode: store.ModeDir, ID: store.EmptyTree}
		}
	}
	for p, f := range t.files {
		if f.Mode == store.ModeDir && parents[p] {
			delete(t.files, p)
		}
	}
	return t.files
}

// hashExcluded keeps .git entries (owned by the git adapter) and AppleDouble sidecars out of trees.
func hashExcluded(p string) bool {
	if source.IsGitPath(p) {
		return true
	}
	for component := range strings.SplitSeq(p, "/") {
		if strings.HasPrefix(component, "._") {
			return true
		}
	}
	return false
}
