package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stencil-hq/ovnfs/admin"
	"github.com/stencil-hq/ovnfs/api"
)

// TestMain answers a spawned `serve` by exiting, so a startDaemon that spawns
// one fails instead of running the tests again in a child process.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		os.Exit(3)
	}
	os.Exit(m.Run())
}

// TestStartWaitsForALoadingDaemon serves an admin socket that answers its
// hello only a second later, as a daemon loading its state does: startDaemon
// waits for that daemon and starts no serve of its own.
func TestStartWaitsForALoadingDaemon(t *testing.T) {
	// A short path: a Unix socket path is limited to about 100 bytes.
	state, err := os.MkdirTemp("", "ovnfs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(state) })
	l, err := admin.Listen(socketPath(state))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	srv := admin.NewServer()
	go srv.Serve(l)
	time.AfterFunc(time.Second, func() { srv.Ready(admin.Welcome{Version: 1}, nil, nil) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, pid, err := startDaemon(ctx, state, filepath.Join(state, "mnt"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if pid != 0 {
		t.Fatalf("started serve pid %d, want none", pid)
	}
	if client.Welcome().Version != 1 {
		t.Fatal("missing welcome")
	}
}

// TestClientQuotesLogOfAFailedLoad drops a held hello, as a daemon whose state
// load fails does: a client that did not start it quotes the daemon log.
func TestClientQuotesLogOfAFailedLoad(t *testing.T) {
	state, err := os.MkdirTemp("", "ovnfs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(state) })
	if err := os.WriteFile(logPath(state), []byte("read state.json: unexpected end of JSON input\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := admin.Listen(socketPath(state))
	if err != nil {
		t.Fatal(err)
	}
	go admin.NewServer().Serve(l)
	time.AfterFunc(200*time.Millisecond, func() { l.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := awaitDaemon(ctx, state, 0, nil)
	if err == nil {
		client.Close()
		t.Fatal("hello answered by a daemon that never loaded")
	}
	if !strings.Contains(err.Error(), "read state.json: unexpected end of JSON input") {
		t.Fatalf("error without the daemon log: %v", err)
	}
}

// TestStatusRowsShowMemoryError shows parked memory files right after the
// memory row, and no row while none waits.
func TestStatusRowsShowMemoryError(t *testing.T) {
	row := func(rows [][2]string, label string) (int, string) {
		for i, r := range rows {
			if r[0] == label {
				return i, r[1]
			}
		}
		return -1, ""
	}
	rows := statusRows(api.Status{MemoryParked: 2, MemoryError: "no space left on device"})
	i, got := row(rows, "memory error")
	if got != "2 parked: no space left on device" {
		t.Fatalf("memory error row = %q", got)
	}
	if m, _ := row(rows, "memory"); i != m+1 {
		t.Fatalf("memory error row at %d, memory row at %d", i, m)
	}
	if i, _ := row(statusRows(api.Status{}), "memory error"); i != -1 {
		t.Fatal("memory error row shown with no parked file")
	}
}
