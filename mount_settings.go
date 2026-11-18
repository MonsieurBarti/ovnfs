package main

import "fmt"

const defaultNFSIOSize = 128 << 10

// mountSettings controls native client mounts. A zero NFSIOSize uses the default.
type mountSettings struct {
	Atime     bool
	NFSIOSize int
	NFSSoft   bool
}

// nfsRecovery is the NFS option for a daemon that stops answering. A hard
// mount (the default) waits for it however long it takes, so every process
// that touches a checkout blocks. A soft one fails the call with ETIMEDOUT
// instead: on macOS after 12 to 18 s (timeo 1 s, three retries with backoff).
// A write that times out is lost, so soft is opt-in.
func (s mountSettings) nfsRecovery() string {
	if s.NFSSoft {
		return "soft,timeo=10,retrans=3"
	}
	return "hard"
}

func (s mountSettings) nfsIOSize() int {
	if s.NFSIOSize == 0 {
		return defaultNFSIOSize
	}
	return s.NFSIOSize
}

func (s mountSettings) validate() error {
	n := s.nfsIOSize()
	if n < 4<<10 || n > 1<<20 || n&(n-1) != 0 {
		return fmt.Errorf("NFS I/O size must be a power of two from 4 KiB to 1 MiB, got %d bytes", n)
	}
	return nil
}
