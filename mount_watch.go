package main

import (
	"context"
	"time"
)

// mountCheckEvery is how often the daemon checks that its system mount exists.
const mountCheckEvery = 10 * time.Second

// mountState is what the system mount records say about the daemon's mount point.
type mountState int

const (
	mountPresent mountState = iota // the daemon's mount
	mountMissing                   // no mount: a forced unmount removed it
	mountForeign                   // a mount the daemon did not make
)

// watchMount checks the mount at mount every period until ctx ends, and runs
// remount while it is missing. A foreign mount is not the daemon's to touch.
// It logs each event once per streak of failed checks, and a remount error
// again only when its text changes; a present mount ends the streak.
func watchMount(ctx context.Context, mount string, every time.Duration, check func() mountState, remount func() error, logf func(string, ...any)) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	var lost, foreign, remounted bool
	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		switch check() {
		case mountPresent:
			lost, foreign, remounted, lastErr = false, false, false, ""
		case mountForeign:
			if !foreign {
				logf("mount at %s is not the daemon's; leaving it alone", mount)
				foreign = true
			}
		case mountMissing:
			if !lost {
				logf("mount lost at %s", mount)
				lost = true
			}
			if err := remount(); err != nil {
				if err.Error() != lastErr {
					logf("remount at %s: %v", mount, err)
					lastErr = err.Error()
				}
			} else if !remounted {
				logf("remounted at %s", mount)
				remounted = true
			}
		}
	}
}
