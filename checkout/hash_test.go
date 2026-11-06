package checkout

import (
	"context"
	"testing"

	"github.com/stencil-hq/ovnfs/api"
	"github.com/stencil-hq/ovnfs/store"
)

// TestNestedEmptyDirectories checks that directories a checkout creates inside
// each other, with no file below them, snapshot as one empty-directory leaf at
// the deepest level, and that apply can read that snapshot.
func TestNestedEmptyDirectories(t *testing.T) {
	isolateGit(t)
	f := newFixture(t)
	dir := folder(t, map[string]string{"a.txt": "a"})
	f.newCheckout(dir, "c")
	s, err := f.reg.Enter("c")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FS.MkdirAll("x/y/z", 0o755); err != nil {
		s.Done()
		t.Fatal(err)
	}
	s.Done()

	reply, err := f.reg.Snap(context.Background(), api.SnapArgs{Checkout: "c", Wait: true})
	if err != nil {
		t.Fatalf("snap: %v", err)
	}
	files, err := f.reg.store.Flatten(store.OID(reply.Snapshot.Tree))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"x", "x/y"} {
		if _, ok := files[p]; ok {
			t.Errorf("%s is a leaf, want only the implied parent of x/y/z", p)
		}
	}
	if leaf, ok := files["x/y/z"]; !ok || leaf.Mode != store.ModeDir || leaf.ID != store.EmptyTree {
		t.Errorf("x/y/z = %+v, %v; want an empty-directory leaf", leaf, ok)
	}

	if _, err := f.reg.Apply(context.Background(), api.ApplyArgs{Checkout: "c", DryRun: true}); err != nil {
		t.Fatalf("apply --dry-run: %v", err)
	}
}
