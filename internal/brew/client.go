package brew

import (
	"sort"
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	ID     string
	Remote string
	// Username is the digest-auth identity captured at /discovery (typically
	// the BlueStation's ISSI as a decimal string). Empty when the server runs
	// with auth disabled. Set once at construction; read-only afterwards.
	Username string
	conn     *websocket.Conn
	send     chan []byte

	mu          sync.RWMutex
	subscribers map[uint32]*subscriberState
}

type subscriberState struct {
	groups map[uint32]struct{}
}

type SubscriberSnapshot struct {
	Number uint32   `json:"number"`
	Groups []uint32 `json:"groups"`
}

type ClientSnapshot struct {
	ID          string               `json:"id"`
	Remote      string               `json:"remote"`
	Username    string               `json:"username,omitempty"`
	Groups      []uint32             `json:"groups"`
	Subscribers []SubscriberSnapshot `json:"subscribers"`
}

func newClient(id, remote, username string, conn *websocket.Conn) *Client {
	return &Client{
		ID:          id,
		Remote:      remote,
		Username:    username,
		conn:        conn,
		send:        make(chan []byte, 128),
		subscribers: make(map[uint32]*subscriberState),
	}
}

func (c *Client) Enqueue(payload []byte) bool {
	select {
	case c.send <- payload:
		return true
	default:
		return false
	}
}

func (c *Client) Close() error {
	close(c.send)
	return c.conn.Close()
}

func (c *Client) ApplySubscriber(msg *SubscriberMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch msg.MsgType {
	case SubscriberRegister, SubscriberReregister:
		c.ensureSubscriber(msg.Number)
	case SubscriberDeregister:
		delete(c.subscribers, msg.Number)
	case SubscriberAffiliate:
		s := c.ensureSubscriber(msg.Number)
		for _, g := range msg.Groups {
			s.groups[g] = struct{}{}
		}
	case SubscriberDeaffiliate:
		s := c.ensureSubscriber(msg.Number)
		for _, g := range msg.Groups {
			delete(s.groups, g)
		}
	}
}

func (c *Client) IsAttachedTo(gssi uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.subscribers {
		if _, ok := s.groups[gssi]; ok {
			return true
		}
	}
	return false
}

func (c *Client) AttachedGroups() []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return collectGroupsNoLock(c.subscribers)
}

func (c *Client) HasSubscriber(number uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.subscribers[number]
	return ok
}

func (c *Client) Snapshot() ClientSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	subs := make([]SubscriberSnapshot, 0, len(c.subscribers))
	for number, state := range c.subscribers {
		groups := make([]uint32, 0, len(state.groups))
		for g := range state.groups {
			groups = append(groups, g)
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
		subs = append(subs, SubscriberSnapshot{
			Number: number,
			Groups: groups,
		})
	}
	sort.Slice(subs, func(i, j int) bool { return subs[i].Number < subs[j].Number })

	return ClientSnapshot{
		ID:          c.ID,
		Remote:      c.Remote,
		Username:    c.Username,
		Groups:      collectGroupsNoLock(c.subscribers),
		Subscribers: subs,
	}
}

func (c *Client) ensureSubscriber(issi uint32) *subscriberState {
	s, ok := c.subscribers[issi]
	if !ok {
		s = &subscriberState{groups: make(map[uint32]struct{})}
		c.subscribers[issi] = s
	}
	return s
}

func collectGroupsNoLock(subscribers map[uint32]*subscriberState) []uint32 {
	groups := make(map[uint32]struct{})
	for _, s := range subscribers {
		for g := range s.groups {
			groups[g] = struct{}{}
		}
	}
	out := make([]uint32, 0, len(groups))
	for g := range groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}
