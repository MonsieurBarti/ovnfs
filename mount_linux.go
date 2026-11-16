//go:build linux

package main

import (
	"os"
	"slices"
	"strings"
)

// mounted matches procfs mount records without querying the mounted
// filesystem; it errs toward true when they cannot be read.
func mounted(dir string) bool {
	_, found, err := mountOptions(dir)
	return found || err != nil
}

// checkMount reports the mount present: the remount watch is macOS NFS only.
func checkMount(_, _, _ string, _ int) mountState { return mountPresent }

// mountAtime reports whether the mount at dir updates access times; ok is
// false when dir is not a mount point or the records cannot be read.
func mountAtime(dir string) (atime, ok bool) {
	opts, found, err := mountOptions(dir)
	if err != nil || !found {
		return false, false
	}
	return !slices.Contains(strings.Split(opts, ","), "noatime"), true
}

// remountCmd updates the mount at dir with generic option opt.
func remountCmd(dir, opt string) []string {
	return []string{"mount", "-o", "remount," + opt, dir}
}

// mountOptions returns the per-mount options of dir's procfs mount record.
func mountOptions(dir string) (string, bool, error) {
	target := mountPath(dir)
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", false, err
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 6 && unescape.Replace(fields[4]) == target {
			return fields[5], true, nil
		}
	}
	return "", false, nil
}
