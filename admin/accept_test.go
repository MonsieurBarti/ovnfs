//go:build darwin || linux || windows

package admin

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// acceptStep answers one Accept call, given the real listener.
type acceptStep func(net.Listener) (net.Conn, error)

// scriptedListener answers each Accept with the next step, then with the real listener.
type scriptedListener struct {
	net.Listener
	steps chan acceptStep
}

// Accept runs the next scripted step, or the real Accept when no step is left.
func (l *scriptedListener) Accept() (net.Conn, error) {
	select {
	case step := <-l.steps:
		return step(l.Listener)
	default:
		return l.Listener.Accept()
	}
}

// realAccept accepts one client on the real listener.
func realAccept(l net.Listener) (net.Conn, error) { return l.Accept() }

// scriptedServe listens on a temp socket and serves it through the steps.
func scriptedServe(t *testing.T, s *Server, steps ...acceptStep) (string, <-chan error) {
	t.Helper()
	dir, err := os.MkdirTemp("", "ovn-admin-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "sock")
	inner, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inner.Close() })
	l := &scriptedListener{Listener: inner, steps: make(chan acceptStep, len(steps))}
	for _, step := range steps {
		l.steps <- step
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve(l) }()
	return path, served
}

// dialAsync dials path and reports the welcome version, or the error.
func dialAsync(path string) <-chan error {
	done := make(chan error, 1)
	go func() {
		c, err := Dial(path, "test")
		if err == nil {
			if c.Welcome().Version != 1 {
				err = errors.New("missing welcome")
			}
			c.Close()
		}
		done <- err
	}()
	return done
}

// TestServeSurvivesATemporaryAcceptError keeps serving after one EMFILE, so
// the next client still gets its welcome.
func TestServeSurvivesATemporaryAcceptError(t *testing.T) {
	s := NewServer()
	logged := make(chan string, 4)
	s.Logf = func(format string, args ...any) { logged <- format }
	emfile := func(net.Listener) (net.Conn, error) {
		return nil, &net.OpError{Op: "accept", Net: "unix", Err: os.NewSyscallError("accept", syscall.EMFILE)}
	}
	path, served := scriptedServe(t, s, emfile, realAccept)
	s.Ready(Welcome{Version: 1}, nil, nil)
	select {
	case err := <-dialAsync(path):
		if err != nil {
			t.Fatal(err)
		}
	case err := <-served:
		t.Fatalf("Serve ended on a temporary accept error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("client got no welcome")
	}
	if n := len(logged); n != 1 {
		t.Fatalf("logged %d lines for one accept error, want 1", n)
	}
}

// TestServeEndsOnAClosedListener returns from Serve on a closed listener and
// closes a client whose hello still waits for Ready.
func TestServeEndsOnAClosedListener(t *testing.T) {
	closed := func(net.Listener) (net.Conn, error) { return nil, net.ErrClosed }
	s := NewServer()
	path, served := scriptedServe(t, s, realAccept, closed)
	held := dialAsync(path)
	select {
	case err := <-served:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Serve returned %v, want net.ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve still running after the listener closed")
	}
	select {
	case err := <-held:
		if err == nil {
			t.Fatal("held hello got a welcome from a server that never became ready")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("held hello still waiting after Serve ended")
	}
}
