//go:build darwin

package main

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestMountStateOf(t *testing.T) {
	const source = "</state/nfs.sock>:/"
	record := func(fstype, from string) unix.Statfs_t {
		var rec unix.Statfs_t
		copy(rec.Fstypename[:], fstype)
		copy(rec.Mntfromname[:], from)
		return rec
	}
	for _, c := range []struct {
		name  string
		rec   unix.Statfs_t
		found bool
		want  mountState
	}{
		{"none", unix.Statfs_t{}, false, mountMissing},
		{"ours", record("nfs", source), true, mountPresent},
		{"other nfs source", record("nfs", "host:/x"), true, mountForeign},
		{"other type", record("apfs", source), true, mountForeign},
	} {
		if got := mountStateOf(c.rec, c.found, source); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}
