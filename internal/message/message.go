package message

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"crossledgerlab/internal/store"
)

const (
	KindLock    = "lock"
	KindClaim   = "claim"
	KindRefund  = "refund"
	KindResolve = "resolve"
	KindAck     = "ack"
)

type Message struct {
	ID          string          `json:"id"`
	From        string          `json:"from"`
	To          string          `json:"to"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	SendAt      time.Time       `json:"send_at"`
	ReadyAt     time.Time       `json:"ready_at"`
	Delivered   bool            `json:"delivered"`
	DeliveredAt *time.Time      `json:"delivered_at,omitempty"`
}

type Record struct {
	Message  Message
	Response *Message
	Err      error
}

type Handler func(Message) (*Message, error)

type Network struct {
	mu       sync.Mutex
	path     string
	messages map[string]*Message
	handlers map[string]Handler
	disabled map[string]bool
	now      func() time.Time
}

func NewNetwork(path string, now func() time.Time) *Network {
	return &Network{path: path, messages: map[string]*Message{}, handlers: map[string]Handler{}, disabled: map[string]bool{}, now: now}
}

func (n *Network) Load() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	var list []*Message
	if err := store.LoadJSON(n.path, &list); err != nil {
		return err
	}
	for _, msg := range list {
		n.messages[msg.ID] = msg
	}
	return nil
}

func (n *Network) Handle(endpoint string, handler Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[endpoint] = handler
}

func (n *Network) SetDisconnected(endpoint string, disconnected bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disabled[endpoint] = disconnected
}

func (n *Network) Send(from, to, kind string, payload any, delay time.Duration) (Message, error) {
	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return Message{}, err
	}
	return n.SendWithID(hex.EncodeToString(idBytes[:]), from, to, kind, payload, delay)
}

// SendWithID 使用调用方给定的确定消息 ID 发送；相同 ID 的消息已存在时直接返回原消息，
// 协调者重启后可用原编号重发同一决定，而不会产生第二笔效果。
func (n *Network) SendWithID(id, from, to, kind string, payload any, delay time.Duration) (Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Message{}, err
	}
	now := n.now().UTC()
	msg := Message{ID: id, From: from, To: to, Kind: kind, Payload: data, SendAt: now, ReadyAt: now.Add(delay)}
	n.mu.Lock()
	if existing := n.messages[id]; existing != nil {
		copy := cloneMessage(existing)
		n.mu.Unlock()
		return copy, nil
	}
	if n.disabled[to] {
		msg.ReadyAt = time.Time{}
	}
	n.messages[msg.ID] = &msg
	err = n.saveLocked()
	n.mu.Unlock()
	return msg, err
}

func (n *Network) Pump() ([]Record, error) {
	n.mu.Lock()
	now := n.now()
	var pending []*Message
	for _, msg := range n.messages {
		if !msg.Delivered && !msg.ReadyAt.IsZero() && !now.Before(msg.ReadyAt) && !n.disabled[msg.To] {
			pending = append(pending, msg)
		}
	}
	handlers := map[string]Handler{}
	for endpoint, handler := range n.handlers {
		handlers[endpoint] = handler
	}
	n.mu.Unlock()

	var records []Record
	for _, original := range pending {
		msg := cloneMessage(original)
		handler := handlers[msg.To]
		if handler == nil {
			continue
		}
		response, err := handler(msg)
		records = append(records, Record{Message: msg, Response: response, Err: err})
		n.markDelivered(msg.ID)
	}
	if len(records) > 0 {
		n.mu.Lock()
		err := n.saveLocked()
		n.mu.Unlock()
		if err != nil {
			return records, err
		}
	}
	return records, nil
}

func (n *Network) RestoreDisconnected() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, msg := range n.messages {
		if !msg.Delivered && msg.ReadyAt.IsZero() && !n.disabled[msg.To] {
			msg.ReadyAt = n.now().UTC()
		}
	}
}

func (n *Network) PendingCount(endpoint string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for _, msg := range n.messages {
		if msg.To == endpoint && !msg.Delivered {
			count++
		}
	}
	return count
}

func (n *Network) markDelivered(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if msg := n.messages[id]; msg != nil {
		now := n.now().UTC()
		msg.Delivered = true
		msg.DeliveredAt = &now
	}
}

func (n *Network) saveLocked() error {
	list := make([]*Message, 0, len(n.messages))
	for _, msg := range n.messages {
		copy := cloneMessage(msg)
		list = append(list, &copy)
	}
	return store.SaveJSON(n.path, list)
}

func cloneMessage(in *Message) Message {
	out := *in
	out.Payload = append(json.RawMessage(nil), in.Payload...)
	return out
}

func Decode[T any](msg Message) (T, error) {
	var value T
	if err := json.Unmarshal(msg.Payload, &value); err != nil {
		return value, fmt.Errorf("decode %s: %w", msg.Kind, err)
	}
	return value, nil
}
