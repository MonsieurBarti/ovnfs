package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stencil-hq/ovnfs/admin"
	"github.com/stencil-hq/ovnfs/api"
	"github.com/stencil-hq/ovnfs/checkout"
	"github.com/stencil-hq/ovnfs/overlay"
)

// TestOpsStatusAndEmptyLs checks reply shapes on a fresh registry and strict argument decoding.
func TestOpsStatusAndEmptyLs(t *testing.T) {
	root := t.TempDir()
	reg, err := checkout.Open(checkout.Config{State: filepath.Join(root, "state"), Mount: filepath.Join(root, "mount"), Memory: overlay.NewMemory(64<<20, 1<<20, 0), BlobCache: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	h := &opsHandler{reg: reg, welcome: admin.Welcome{Proto: "nfs", Port: 1234, Mount: "/mount", State: "/state"}, started: time.Now()}
	status, err := h.Call(context.Background(), "status", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if s := status.(api.Status); s.Version != protocolVersion || s.Port != 1234 || s.Proto != "nfs" {
		t.Fatalf("status: %#v", s)
	}
	rawStatus, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the wire protocol version numeric and the daemon's build separate.
	var wire struct {
		Version      int    `json:"version"`
		BuildVersion string `json:"build_version"`
	}
	if err := json.Unmarshal(rawStatus, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Version != protocolVersion || wire.BuildVersion != processBuild.Version {
		t.Fatalf("status wire versions: %s", rawStatus)
	}
	list, err := h.Call(context.Background(), "ls", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "null") {
		t.Fatalf("null collections: %s", b)
	}
	for _, bad := range []string{`null`, `[]`, `{"unknown":1}`, `{} {}`} {
		if _, err = h.Call(context.Background(), "status", json.RawMessage(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	if _, err = h.Call(context.Background(), "new", json.RawMessage(`{"dir":"/x","rules":"source *"}`)); err == nil {
		t.Error("accepted a string where rule lines are expected")
	}
	if _, err = h.Call(context.Background(), "bogus", json.RawMessage(`{}`)); err == nil {
		t.Fatal("accepted unsupported op")
	}
}

// TestOpsStatusReportsMountLost checks that status reports the mount check.
func TestOpsStatusReportsMountLost(t *testing.T) {
	root := t.TempDir()
	reg, err := checkout.Open(checkout.Config{State: filepath.Join(root, "state"), Mount: filepath.Join(root, "mount"), Memory: overlay.NewMemory(64<<20, 1<<20, 0), BlobCache: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	for _, lost := range []bool{true, false} {
		h := &opsHandler{reg: reg, started: time.Now()}
		if lost {
			h.mountLost = func() bool { return true }
		}
		status, err := h.Call(context.Background(), "status", json.RawMessage(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if got := status.(api.Status).MountLost; got != lost {
			t.Errorf("MountLost %v, want %v", got, lost)
		}
	}
}

// TestEventFanout checks that independent subscribers see the same persisted event.
func TestEventFanout(t *testing.T) {
	hub := &eventHub{}
	ctx := t.Context()
	a, b := hub.events(ctx), hub.events(ctx)
	hub.emit(checkout.Event{Name: "created", Payload: map[string]string{"name": "a"}})
	for _, ch := range []<-chan admin.Event{a, b} {
		select {
		case e := <-ch:
			if e.Name != "created" {
				t.Fatal(e)
			}
		case <-time.After(time.Second):
			t.Fatal("event not delivered")
		}
	}
}
