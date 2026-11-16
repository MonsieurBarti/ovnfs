package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/stencil-hq/ovnfs/admin"
	"github.com/stencil-hq/ovnfs/api"
	"github.com/stencil-hq/ovnfs/checkout"
)

// protocolVersion is the admin protocol generation: one op per CLI verb, shapes in package api.
const protocolVersion = 5

// opsHandler binds admin operations to one checkout registry.
type opsHandler struct {
	reg       *checkout.Registry
	shares    *shares
	links     *links
	welcome   admin.Welcome
	started   time.Time
	stop      func(context.Context, api.StopArgs) error // unmounts as asked, then has the server exit
	mountLost func() bool                               // reports the system mount gone; nil where the daemon does not watch it
}

// decodeArgs rejects malformed or unknown request fields.
func decodeArgs(raw json.RawMessage, v any) error {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return &checkout.Error{Code: "invalid", Message: "arguments must be a JSON object"}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return &checkout.Error{Code: "invalid", Message: err.Error()}
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return &checkout.Error{Code: "invalid", Message: "expected one JSON object"}
	}
	return nil
}

// dispatch decodes an operation's arguments and runs it.
func dispatch[A, R any](ctx context.Context, raw json.RawMessage, fn func(context.Context, A) (R, error)) (any, error) {
	var args A
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	return fn(ctx, args)
}

// done adapts operations that reply with nothing.
func done[A any](fn func(context.Context, A) error) func(context.Context, A) (struct{}, error) {
	return func(ctx context.Context, a A) (struct{}, error) { return struct{}{}, fn(ctx, a) }
}

// Call dispatches one operation; see package api for argument and reply shapes.
func (h *opsHandler) Call(ctx context.Context, op string, raw json.RawMessage) (any, error) {
	switch op {
	case "status":
		return dispatch(ctx, raw, func(context.Context, struct{}) (api.Status, error) {
			s := api.Status{Version: protocolVersion, BuildVersion: processBuild.Version, PID: os.Getpid(), Uptime: time.Since(h.started).Seconds(), Proto: h.welcome.Proto, Port: h.welcome.Port, Mount: h.welcome.Mount, State: h.welcome.State, Socket: socketPath(h.welcome.State)}
			h.reg.Counts(&s)
			reportHeap(&s)
			if h.mountLost != nil {
				s.MountLost = h.mountLost()
			}
			if w := vnodeWarning(h.reg.Largest()); w != "" {
				s.Warnings = append(s.Warnings, w)
			}
			return s, nil
		})
	case "ls":
		return dispatch(ctx, raw, h.reg.Ls)
	case "new":
		return dispatch(ctx, raw, h.reg.New)
	case "fork":
		return dispatch(ctx, raw, h.reg.Fork)
	case "snap":
		return dispatch(ctx, raw, h.reg.Snap)
	case "diff":
		return dispatch(ctx, raw, h.reg.Diff)
	case "grep":
		return dispatch(ctx, raw, h.reg.Grep)
	case "glob":
		return dispatch(ctx, raw, h.reg.Glob)
	case "apply":
		return dispatch(ctx, raw, h.reg.Apply)
	case "land":
		return dispatch(ctx, raw, h.reg.Land)
	case "rm":
		return dispatch(ctx, raw, done(h.reg.Rm))
	case "prune":
		return dispatch(ctx, raw, h.reg.Prune)
	case "drop":
		return dispatch(ctx, raw, done(h.reg.Drop))
	case "explain":
		return dispatch(ctx, raw, h.reg.Explain)
	case "plan":
		return dispatch(ctx, raw, h.reg.Plan)
	case "profile":
		return dispatch(ctx, raw, captureProfile)
	case "attach":
		return dispatch(ctx, raw, h.links.attach)
	case "pull":
		return dispatch(ctx, raw, h.links.pull)
	case "push":
		return dispatch(ctx, raw, h.links.push)
	case "share":
		return dispatch(ctx, raw, h.shares.Share)
	case "subscribe":
		return dispatch(ctx, raw, func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil })
	case "stop":
		return dispatch(ctx, raw, done(h.stop))
	default:
		return nil, &checkout.Error{Code: "unsupported", Message: fmt.Sprintf("unsupported operation %q", op)}
	}
}
