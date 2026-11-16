// Package admin implements the daemon's local control protocol.
package admin

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

// maxFrame bounds memory consumed by a single protocol message.
const maxFrame = 16 << 20

var errFrameTooLarge = errors.New("admin frame too large")

// Welcome describes the server and negotiated operations.
type Welcome struct {
	Version  int      `json:"version"`
	Ops      []string `json:"ops"`
	Features []string `json:"features,omitempty"`
	Proto    string   `json:"proto"`
	Port     int      `json:"port"`
	Mount    string   `json:"mount"`
	State    string   `json:"state"`
}

// Handler executes one admin request.
type Handler interface {
	Call(context.Context, string, json.RawMessage) (any, error)
}

// Event is an unsolicited daemon state notification.
type Event struct {
	Name    string
	Payload any
}

// RemoteError is a protocol error returned by the server.
type RemoteError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error describes the remote failure.
func (e *RemoteError) Error() string { return e.Code + ": " + e.Message }

// invalidFrame identifies a consumed frame with malformed JSON.
type invalidFrame struct{ err error }

// Error describes malformed frame contents.
func (e *invalidFrame) Error() string { return e.err.Error() }

// readFrame validates and decodes one bounded JSON object.
func readFrame(r io.Reader) (map[string]json.RawMessage, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(header[:])
	if n > maxFrame {
		return nil, fmt.Errorf("admin frame exceeds %d bytes", maxFrame)
	}
	data := make([]byte, int(n))
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	if !utf8.Valid(data) {
		return nil, &invalidFrame{errors.New("invalid UTF-8")}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, &invalidFrame{err}
	}
	if obj == nil {
		return nil, &invalidFrame{errors.New("expected JSON object")}
	}
	return obj, nil
}

// writeFrame writes a complete bounded frame, including short-write handling.
func writeFrame(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > maxFrame {
		return errFrameTooLarge
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(data)))
	for _, buf := range [][]byte{header[:], data} {
		for len(buf) > 0 {
			n, err := w.Write(buf)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			buf = buf[n:]
		}
	}
	return nil
}

// Serve accepts authenticated local clients until the listener is closed.
func Serve(l net.Listener, w Welcome, h Handler, events func(context.Context) <-chan Event) error {
	s := NewServer()
	s.Ready(w, h, events)
	return s.Serve(l)
}

// Server answers admin clients for a daemon that can listen before it can
// answer. Serve accepts each connection at once, so the socket shows a
// starting daemon, and holds its hello until Ready supplies the welcome.
type Server struct {
	ready  chan struct{}
	closed chan struct{}
	w      Welcome
	h      Handler
	events func(context.Context) <-chan Event
	// Logf, when set, reports each temporary accept error that Serve retries.
	Logf func(format string, args ...any)
}

// NewServer returns a server that holds every hello until Ready.
func NewServer() *Server {
	return &Server{ready: make(chan struct{}), closed: make(chan struct{})}
}

// Ready answers held and later hellos with w, and their requests with h. Call it once.
func (s *Server) Ready(w Welcome, h Handler, events func(context.Context) <-chan Event) {
	s.w, s.h, s.events = w, h, events
	close(s.ready)
}

// Serve accepts clients on l until it is closed. A connection still waiting
// for Ready then closes, so its client fails instead of waiting on. A
// temporary accept error, such as running out of descriptors, is retried
// after a backoff; any other error ends Serve.
func (s *Server) Serve(l net.Listener) error {
	defer close(s.closed)
	var delay time.Duration
	for {
		conn, err := l.Accept()
		if err != nil {
			if !temporaryAcceptError(err) {
				return err
			}
			delay = min(max(2*delay, 5*time.Millisecond), time.Second)
			if s.Logf != nil {
				s.Logf("accept: %v, retrying in %v", err, delay)
			}
			time.Sleep(delay)
			continue
		}
		delay = 0
		go s.serveConn(conn)
	}
}

// temporaryAcceptError reports whether err is a resource shortage or an
// aborted client that a later Accept can get past.
func temporaryAcceptError(err error) bool {
	for _, errno := range []syscall.Errno{syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM, syscall.ECONNABORTED} {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
}

// serveConn owns a connection and cancels its requests on disconnect.
func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	if err := CheckPeer(conn); err != nil {
		return
	}
	select {
	case <-s.ready:
	case <-s.closed:
		return
	}
	w, h, events := s.w, s.h, s.events
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	send := func(v any) error {
		mu.Lock()
		defer mu.Unlock()
		err := writeFrame(conn, v)
		if err != nil && !errors.Is(err, errFrameTooLarge) {
			cancel()
			conn.Close()
		}
		return err
	}
	first, err := readFrame(conn)
	if err != nil || len(first) != 1 {
		return
	}
	hello, ok := first["hello"]
	if !ok || !isObject(hello) {
		return
	}
	if send(map[string]any{"welcome": w}) != nil {
		return
	}
	allowed := make(map[string]bool, len(w.Ops))
	for _, op := range w.Ops {
		allowed[op] = true
	}
	var requestsMu sync.Mutex
	requests := make(map[uint64]context.CancelFunc)
	var subscribed bool
	for {
		obj, err := readFrame(conn)
		if err != nil {
			if _, ok := errors.AsType[*invalidFrame](err); ok {
				if send(map[string]any{"id": 0, "error": &RemoteError{Code: "invalid", Message: err.Error()}}) != nil {
					return
				}
				continue
			}
			return
		}
		if raw, ok := obj["cancel"]; ok {
			var id uint64
			if len(obj) != 1 || json.Unmarshal(raw, &id) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				if send(map[string]any{"id": 0, "error": &RemoteError{Code: "invalid", Message: "expected cancel with one request id"}}) != nil {
					return
				}
				continue
			}
			requestsMu.Lock()
			if cancelRequest := requests[id]; cancelRequest != nil {
				cancelRequest()
			}
			requestsMu.Unlock()
			continue
		}
		var id uint64
		rawID, hasID := obj["id"]
		valid := hasID && json.Unmarshal(rawID, &id) == nil && !bytes.Equal(bytes.TrimSpace(rawID), []byte("null")) && len(obj) == 2
		op := ""
		var args json.RawMessage
		for k, v := range obj {
			if k != "id" {
				op, args = k, v
			}
		}
		fail := func(code, msg string) {
			_ = send(map[string]any{"id": id, "error": &RemoteError{Code: code, Message: msg}})
		}
		if !valid || op == "" || !isObject(args) {
			fail("invalid", "expected id and one operation with object arguments")
			continue
		}
		if !allowed[op] {
			fail("unsupported", "unsupported operation: "+op)
			continue
		}
		requestsMu.Lock()
		if _, exists := requests[id]; exists {
			requestsMu.Unlock()
			fail("invalid", "request id already active")
			continue
		}
		if op != "subscribe" {
			requestCtx, cancelRequest := context.WithCancel(ctx)
			// Register before spawning so an immediately following control
			// frame can cancel a handler that has not started yet.
			requests[id] = cancelRequest
			requestsMu.Unlock()
			go func() {
				result, err := h.Call(requestCtx, op, args)
				// Completion and cancellation share a lock so an accepted
				// cancellation cannot be replaced by a successful reply.
				requestsMu.Lock()
				if requestErr := requestCtx.Err(); requestErr != nil {
					err = requestErr
				}
				delete(requests, id)
				requestsMu.Unlock()
				cancelRequest()
				if err != nil {
					code := "io"
					var coded interface{ ErrorCode() string }
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						code = "canceled"
					} else if errors.As(err, &coded) {
						code = coded.ErrorCode()
					}
					_ = send(map[string]any{"id": id, "error": &RemoteError{Code: code, Message: err.Error()}})
					return
				}
				if result == nil {
					result = struct{}{}
				}
				if err := send(map[string]any{"id": id, "ok": result}); errors.Is(err, errFrameTooLarge) {
					_ = send(map[string]any{"id": id, "error": &RemoteError{Code: "too_large", Message: "reply exceeds 16 MiB; request fewer results or narrower output"}})
				}
			}()
			continue
		}
		requestsMu.Unlock()
		// The subscription outlives its acknowledgment request and remains
		// tied to the connection context.
		if subscribed {
			fail("invalid", "already subscribed")
			continue
		}
		subscribed = true
		var ch <-chan Event
		if events != nil {
			ch = events(ctx)
		}
		if send(map[string]any{"id": id, "ok": struct{}{}}) != nil {
			return
		}
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-ch:
					if !ok {
						return
					}
					if send(map[string]any{"event": map[string]any{event.Name: event.Payload}}) != nil {
						return
					}
				}
			}
		}()
	}
}

// isObject requires operation arguments to be JSON objects rather than null.
func isObject(raw json.RawMessage) bool {
	var obj map[string]json.RawMessage
	return json.Unmarshal(raw, &obj) == nil && obj != nil
}

// response routes one reply or a connection failure to its caller.
type response struct {
	value json.RawMessage
	err   error
}

// Client multiplexes requests over an authenticated local connection.
type Client struct {
	conn         net.Conn
	welcome      Welcome
	writeMu      sync.Mutex
	mu           sync.Mutex
	next         uint64
	pending      map[uint64]chan response
	done         chan struct{}
	err          error
	eventQueue   []json.RawMessage
	eventWake    chan struct{}
	subscribed   bool
	eventStopped bool
}

// Dial connects and completes hello/welcome negotiation.
func Dial(path, client string) (*Client, error) {
	conn, err := net.Dial("unix", path)
	if err != nil {
		return nil, err
	}
	if err = writeFrame(conn, map[string]any{"hello": map[string]string{"client": client}}); err != nil {
		conn.Close()
		return nil, err
	}
	obj, err := readFrame(conn)
	var w Welcome
	if err == nil {
		raw, ok := obj["welcome"]
		if !ok || len(obj) != 1 {
			err = errors.New("expected welcome")
		} else {
			err = json.Unmarshal(raw, &w)
		}
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &Client{conn: conn, welcome: w, pending: make(map[uint64]chan response), done: make(chan struct{}), eventWake: make(chan struct{}, 1)}
	go c.readLoop()
	return c, nil
}

// Welcome returns a copy of the negotiated server capabilities.
func (c *Client) Welcome() Welcome { w := c.welcome; w.Ops = append([]string(nil), w.Ops...); return w }

// Close releases the connection and cancels outstanding calls.
func (c *Client) Close() error { c.shutdown(net.ErrClosed); return nil }

// shutdown completes every pending call exactly once.
func (c *Client) shutdown(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	c.err = err
	close(c.done)
	c.conn.Close()
	for id, ch := range c.pending {
		ch <- response{err: err}
		delete(c.pending, id)
	}
}

// readLoop dispatches replies independently of their arrival order.
func (c *Client) readLoop() {
	for {
		obj, err := readFrame(c.conn)
		if err != nil {
			c.shutdown(err)
			return
		}
		if event, ok := obj["event"]; ok {
			c.mu.Lock()
			if c.subscribed && !c.eventStopped {
				c.eventQueue = append(c.eventQueue, event)
			}
			c.mu.Unlock()
			select {
			case c.eventWake <- struct{}{}:
			default:
			}
			continue
		}
		var id uint64
		if err := json.Unmarshal(obj["id"], &id); err != nil {
			c.shutdown(errors.New("invalid reply id"))
			return
		}
		reply := response{value: obj["ok"]}
		if raw, ok := obj["error"]; ok {
			var remote RemoteError
			if err := json.Unmarshal(raw, &remote); err != nil {
				c.shutdown(err)
				return
			}
			reply.err = &remote
		} else if reply.value == nil {
			c.shutdown(errors.New("missing reply result"))
			return
		}
		c.mu.Lock()
		ch := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if ch != nil {
			ch <- reply
		}
	}
}

// Call sends a request and waits for its corresponding reply or cancellation.
// Canceling ctx notifies the server without closing the shared connection.
func (c *Client) Call(ctx context.Context, op string, args any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	supported := slices.Contains(c.welcome.Ops, op)
	if !supported {
		return nil, &RemoteError{Code: "unsupported", Message: "unsupported operation: " + op}
	}
	if args == nil {
		args = struct{}{}
	}
	ch := make(chan response, 1)
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	c.next++
	id := c.next
	c.pending[id] = ch
	c.mu.Unlock()
	c.writeMu.Lock()
	if err := ctx.Err(); err != nil {
		c.writeMu.Unlock()
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}
	err := writeFrame(c.conn, map[string]any{"id": id, op: args})
	c.writeMu.Unlock()
	if err != nil {
		c.shutdown(err)
	}
	select {
	case r := <-ch:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return r.value, r.err
	case <-ctx.Done():
		err := ctx.Err()
		c.mu.Lock()
		// A reply or shutdown may already have claimed this request. Only
		// outstanding requests still need a cancellation frame.
		_, pending := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if pending {
			c.writeMu.Lock()
			writeErr := writeFrame(c.conn, map[string]any{"cancel": id})
			c.writeMu.Unlock()
			if writeErr != nil {
				c.shutdown(writeErr)
			}
		}
		return nil, err
	}
}

// Subscribe starts a single event stream, closed on cancellation or disconnect.
func (c *Client) Subscribe(ctx context.Context) (<-chan json.RawMessage, error) {
	c.mu.Lock()
	if c.subscribed {
		c.mu.Unlock()
		return nil, errors.New("already subscribed")
	}
	c.subscribed = true
	c.mu.Unlock()
	if _, err := c.Call(ctx, "subscribe", struct{}{}); err != nil {
		c.mu.Lock()
		c.subscribed = false
		c.mu.Unlock()
		return nil, err
	}
	out := make(chan json.RawMessage)
	go func() {
		defer close(out)
		defer func() { c.mu.Lock(); c.eventStopped = true; c.eventQueue = nil; c.mu.Unlock() }()
		for {
			c.mu.Lock()
			var event json.RawMessage
			if len(c.eventQueue) > 0 {
				event = c.eventQueue[0]
				c.eventQueue[0] = nil
				c.eventQueue = c.eventQueue[1:]
			}
			c.mu.Unlock()
			if event != nil {
				select {
				case out <- event:
				case <-ctx.Done():
					return
				case <-c.done:
					return
				}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-c.done:
				return
			case <-c.eventWake:
			}
		}
	}()
	return out, nil
}
