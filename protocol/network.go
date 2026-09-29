package protocol

import (
	"context"
	"sync"
	"time"
)

type MessageStore interface {
	LoadMessages(context.Context) (map[string]Message, error)
	SaveMessages(context.Context, map[string]Message) error
}

type Network struct {
	store    MessageStore
	clock    Clock
	mu       sync.Mutex
	disabled map[Side]bool
	delay    map[Side]time.Duration
	pending  map[string]time.Time
}

func NewNetwork(store MessageStore, clock Clock) *Network {
	if clock == nil {
		clock = RealClock{}
	}
	n := &Network{
		store:    store,
		clock:    clock,
		disabled: map[Side]bool{},
		delay:    map[Side]time.Duration{},
		pending:  map[string]time.Time{},
	}
	messages, err := store.LoadMessages(context.Background())
	if err == nil {
		for id, message := range messages {
			if message.DeliveredAt.IsZero() {
				n.pending[id] = message.CreatedAt
			}
		}
	}
	return n
}

func (n *Network) SetDisconnected(side Side, disconnected bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disabled[side] = disconnected
}

func (n *Network) SetDelay(side Side, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay[side] = delay
}

func (n *Network) Send(ctx context.Context, message Message) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	messages, err := n.store.LoadMessages(ctx)
	if err != nil {
		return false, err
	}
	if _, exists := messages[message.ID]; exists {
		return false, nil
	}
	message.CreatedAt = n.clock.Now()
	messages[message.ID] = message
	if err := n.store.SaveMessages(ctx, messages); err != nil {
		return false, err
	}
	if n.disabled[message.To] {
		n.pending[message.ID] = message.CreatedAt.Add(n.delay[message.To])
		return false, nil
	}
	delay := n.delay[message.To]
	if delay > 0 {
		n.pending[message.ID] = message.CreatedAt.Add(delay)
		return false, nil
	}
	message.DeliveredAt = message.CreatedAt
	delete(n.pending, message.ID)
	return true, n.markDelivered(ctx, message.ID, message.DeliveredAt)
}

func (n *Network) DeliverDue(ctx context.Context) (int, error) {
	n.mu.Lock()
	now := n.clock.Now()
	ids := make([]string, 0)
	for id, due := range n.pending {
		if !now.Before(due) {
			ids = append(ids, id)
		}
	}
	n.mu.Unlock()
	delivered := 0
	for _, id := range ids {
		if err := n.deliver(id); err != nil {
			return delivered, err
		}
		delivered++
	}
	return delivered, nil
}

func (n *Network) markDelivered(ctx context.Context, id string, at time.Time) error {
	messages, err := n.store.LoadMessages(ctx)
	if err != nil {
		return err
	}
	message, ok := messages[id]
	if !ok {
		return nil
	}
	message.DeliveredAt = at
	messages[id] = message
	return n.store.SaveMessages(ctx, messages)
}

func (n *Network) deliver(id string) error {
	n.mu.Lock()
	_, ok := n.pending[id]
	n.mu.Unlock()
	if !ok {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	messages, err := n.store.LoadMessages(context.Background())
	if err != nil {
		return err
	}
	message, exists := messages[id]
	if !exists || n.disabled[message.To] {
		return nil
	}
	delete(n.pending, id)
	return n.markDelivered(context.Background(), id, n.clock.Now())
}

func (n *Network) Flush(ctx context.Context) (int, error) {
	n.mu.Lock()
	ids := make([]string, 0, len(n.pending))
	for id := range n.pending {
		ids = append(ids, id)
	}
	n.mu.Unlock()
	delivered := 0
	for _, id := range ids {
		if err := n.deliver(id); err != nil {
			return delivered, err
		}
		delivered++
	}
	return delivered, nil
}

func (n *Network) Messages(ctx context.Context) ([]Message, error) {
	messages, err := n.store.LoadMessages(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Message, 0, len(messages))
	for _, message := range messages {
		result = append(result, message)
	}
	return result, nil
}
