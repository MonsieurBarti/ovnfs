//go:build darwin || linux

package main

import (
	"net"
	"slices"
	"strings"
	"testing"
)

func TestMountCommandSMBIgnoresNFSIOSize(t *testing.T) {
	addr := &net.TCPAddr{Port: 1234}
	original := mountCmd("smb", addr, "/mounted", mountSettings{}, "ovnfs", "", "")
	changed := mountCmd("smb", addr, "/mounted", mountSettings{NFSIOSize: 1 << 20}, "ovnfs", "", "")
	if !slices.Equal(original, changed) {
		t.Fatalf("NFS size changed SMB command: %v vs %v", original, changed)
	}
}

// TestMountCommandNFSRecovery mounts NFS hard unless soft is set, over both
// the macOS Unix socket and TCP, so a restart replaces a mount of the other kind.
func TestMountCommandNFSRecovery(t *testing.T) {
	for _, addr := range []net.Addr{&net.UnixAddr{Name: "/state/nfs.sock", Net: "unix"}, &net.TCPAddr{Port: 1234}} {
		for _, tc := range []struct {
			soft       bool
			want, deny string
		}{{false, "hard", "soft"}, {true, "soft,timeo=10,retrans=3", "hard"}} {
			cmd := mountCmd("nfs", addr, "/mounted", mountSettings{NFSSoft: tc.soft}, "ovnfs", "", "")
			o := cmd[slices.Index(cmd, "-o")+1]
			if !strings.Contains(","+o+",", ","+tc.want+",") || slices.Contains(strings.Split(o, ","), tc.deny) {
				t.Errorf("%s soft=%v: options %q, want %q and no %q", addr.Network(), tc.soft, o, tc.want, tc.deny)
			}
		}
	}
}
