package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// watchRun drives watchMount until check has answered every state in states,
// then returns how often remount ran and the lines logf got. Checks after the
// last state answer mountPresent.
func watchRun(t *testing.T, states []mountState, remountErrs []error) (int, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	checks, remounts := 0, 0
	var lines []string
	check := func() mountState {
		checks++
		if checks >= len(states) {
			cancel()
		}
		if checks > len(states) {
			return mountPresent
		}
		return states[checks-1]
	}
	remount := func() error {
		remounts++
		if remounts <= len(remountErrs) {
			return remountErrs[remounts-1]
		}
		return nil
	}
	logf := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	watchMount(ctx, "/m", 5*time.Millisecond, check, remount, logf)
	if checks < len(states) {
		t.Fatalf("watchMount returned after %d of %d checks", checks, len(states))
	}
	return remounts, lines
}

// count reports how many lines contain s.
func count(lines []string, s string) int {
	n := 0
	for _, l := range lines {
		if strings.Contains(l, s) {
			n++
		}
	}
	return n
}

func TestWatchMountRemountsOnce(t *testing.T) {
	boom := errors.New("mount_nfs: boom")
	remounts, lines := watchRun(t, []mountState{mountMissing, mountMissing, mountMissing, mountPresent}, []error{boom, boom})
	if remounts != 3 {
		t.Errorf("remount ran %d times, want 3", remounts)
	}
	if len(lines) != 3 || count(lines, "mount lost at /m") != 1 || count(lines, "boom") != 1 || count(lines, "remounted at /m") != 1 {
		t.Errorf("log: %q", lines)
	}
}

func TestWatchMountLogsChangedError(t *testing.T) {
	_, lines := watchRun(t, []mountState{mountMissing, mountMissing, mountMissing, mountPresent}, []error{errors.New("a"), errors.New("b")})
	if len(lines) != 4 || count(lines, "remounted at /m") != 1 {
		t.Errorf("log: %q", lines)
	}
}

func TestWatchMountForeignNotRemounted(t *testing.T) {
	remounts, lines := watchRun(t, []mountState{mountForeign, mountForeign, mountForeign}, nil)
	if remounts != 0 || len(lines) != 1 {
		t.Errorf("remounts %d, log %q", remounts, lines)
	}
}

func TestWatchMountStreaksReset(t *testing.T) {
	remounts, lines := watchRun(t, []mountState{mountMissing, mountPresent, mountMissing, mountPresent}, nil)
	if remounts != 2 || count(lines, "mount lost at /m") != 2 || count(lines, "remounted at /m") != 2 {
		t.Errorf("remounts %d, log %q", remounts, lines)
	}
}
