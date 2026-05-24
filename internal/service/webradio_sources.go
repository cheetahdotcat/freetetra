package service

import (
	"errors"
	"strings"
	"sync"

	"github.com/freetetra/server/internal/config"
)

// errStallTimeout signals that the source watchdog gave up on the current
// URL because no frames have arrived for cfg.WebRadio.StallTimeout. The
// runLoop checks for this with errors.Is and rotates without applying the
// usual reconnect delay — failover should be snappy.
var (
	errStallTimeout = errors.New("webradio: source stalled")
	errManualSkip   = errors.New("webradio: manual skip")
	errManualReload = errors.New("webradio: manual reload")
)

// sourceRotator tracks which entry in the failover list is currently the
// active one. Skip() advances; Current() reads. The struct is safe for
// concurrent use because the bridge reads from one goroutine (runLoop) and
// the live-control endpoints (task 8) call Skip from HTTP handlers.
type sourceRotator struct {
	mu      sync.RWMutex
	sources []string
	idx     int
}

func newSourceRotator(cfg config.WebRadioConfig) *sourceRotator {
	sources := make([]string, 0, len(cfg.Sources)+1)
	for _, s := range cfg.Sources {
		if t := strings.TrimSpace(s); t != "" {
			sources = append(sources, t)
		}
	}
	// Legacy single-source path: if WEBRADIO_SOURCES wasn't set, fall back
	// to the lone StreamURL so existing deployments keep working.
	if len(sources) == 0 {
		if t := strings.TrimSpace(cfg.StreamURL); t != "" {
			sources = append(sources, t)
		}
	}
	return &sourceRotator{sources: sources}
}

func (r *sourceRotator) Current() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sources) == 0 {
		return ""
	}
	return r.sources[r.idx]
}

// Skip advances to the next source and returns its URL. Wraps around the
// list when the end is reached.
func (r *sourceRotator) Skip() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sources) == 0 {
		return ""
	}
	r.idx = (r.idx + 1) % len(r.sources)
	return r.sources[r.idx]
}

func (r *sourceRotator) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sources)
}
