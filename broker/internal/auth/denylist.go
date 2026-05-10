package auth

import "sync"

// DenyList is a set of subjects whose credentials must be rejected
// even when otherwise valid. Implementations are read on the
// verifier's hot path, so Contains must be safe for concurrent use.
type DenyList interface {
	Contains(subject string) bool
}

// MemoryDenyList is the default DenyList implementation: an in-memory
// set of subjects, replaced atomically by Set. The verifier holds it
// by interface, so the daemon's SIGHUP-reload path (PR 4) just calls
// Set with the freshly-parsed file contents — no verifier
// reconstruction needed.
type MemoryDenyList struct {
	mu       sync.RWMutex
	subjects map[string]struct{}
}

// NewMemoryDenyList returns a deny list pre-populated with the given
// subjects.
func NewMemoryDenyList(subjects ...string) *MemoryDenyList {
	d := &MemoryDenyList{}
	d.Set(subjects)
	return d
}

// Contains reports whether subject is denied. Safe for concurrent
// use.
func (d *MemoryDenyList) Contains(subject string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, found := d.subjects[subject]
	return found
}

// Set replaces the list with the supplied subjects under the write
// lock. Suitable for SIGHUP-driven reloads from a file: no in-flight
// Verify call can observe a half-loaded set, because the new map is
// built outside the critical section and swapped in atomically with
// respect to the lock.
func (d *MemoryDenyList) Set(subjects []string) {
	next := make(map[string]struct{}, len(subjects))
	for _, s := range subjects {
		if s == "" {
			continue
		}
		next[s] = struct{}{}
	}
	d.mu.Lock()
	d.subjects = next
	d.mu.Unlock()
}
