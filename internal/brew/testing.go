package brew

import (
	"log"
	"time"
)

// NewTestServer returns a Server suitable for unit tests in other packages.
// It has no HTTP listener, no auth, and an empty client registry. Use
// RegisterTestClient to populate it.
func NewTestServer(logger *log.Logger) *Server {
	return &Server{
		logger:  logger,
		clients: map[string]*Client{},
		nonces:  map[string]time.Time{},
		tokens:  map[string]tokenEntry{},
	}
}

// RegisterTestClient injects a fake client with the given ID and subscribers
// (each entry of subs registers an ISSI affiliated to the given GSSIs).
// Returns the Client so the test can inspect its send queue via DrainSend.
func RegisterTestClient(s *Server, id string, subs map[uint32][]uint32) *Client {
	c := &Client{
		ID:          id,
		Remote:      "test",
		send:        make(chan []byte, 32),
		subscribers: make(map[uint32]*subscriberState),
	}
	for issi, gssis := range subs {
		st := c.ensureSubscriber(issi)
		for _, g := range gssis {
			st.groups[g] = struct{}{}
		}
	}
	s.clientsMu.Lock()
	s.clients[id] = c
	s.clientsMu.Unlock()
	return c
}

// DrainSend returns and removes every payload currently queued on the
// client's send channel. Useful in tests to assert exactly what a broadcast
// delivered.
func DrainSend(c *Client) [][]byte {
	var out [][]byte
	for {
		select {
		case b := <-c.send:
			out = append(out, b)
		default:
			return out
		}
	}
}
