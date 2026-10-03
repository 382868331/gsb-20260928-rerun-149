package node

import (
	"encoding/json"
	"path/filepath"
	"sync"
	"time"

	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/message"
	"crossledgerlab/internal/store"
)

type LockPayload struct {
	EscrowID string    `json:"escrow_id"`
	Transfer string    `json:"transfer_id"`
	From     string    `json:"from"`
	To       string    `json:"to"`
	Amount   uint64    `json:"amount"`
	HashLock []byte    `json:"hash_lock"`
	Deadline time.Time `json:"deadline"`
}

type SecretPayload struct {
	EscrowID string `json:"escrow_id"`
	ClaimID  string `json:"claim_id"`
	Amount   uint64 `json:"amount"`
	Secret   []byte `json:"secret"`
}

type IDPayload struct {
	EscrowID string `json:"escrow_id"`
}

type AckPayload struct {
	Correlation    string `json:"correlation"`
	Status         string `json:"status"`
	ConfirmedTotal uint64 `json:"confirmed_total,omitempty"`
	Remaining      uint64 `json:"remaining,omitempty"`
}

type Node struct {
	mu       sync.Mutex
	path     string
	endpoint string
	ledger   *ledger.Ledger
	handled  map[string]bool
}

func New(endpoint string, l *ledger.Ledger, dir string) *Node {
	return &Node{path: filepath.Join(dir, "messages.json"), endpoint: endpoint, ledger: l, handled: map[string]bool{}}
}

func (n *Node) Load() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return store.LoadJSON(n.path, &n.handled)
}

func (n *Node) Register(net *message.Network) {
	n.Load()
	net.Handle(n.endpoint, n.Handle)
}

func (n *Node) Handle(msg message.Message) (*message.Message, error) {
	n.mu.Lock()
	alreadyHandled := n.handled[msg.ID]
	n.mu.Unlock()
	if alreadyHandled {
		return nil, nil
	}

	ack := AckPayload{Correlation: msg.ID, Status: "ok"}
	var handleErr error
	switch msg.Kind {
	case message.KindLock:
		p, err := message.Decode[LockPayload](msg)
		if err == nil {
			_, err = n.ledger.Lock(ledger.LockRequest{ID: p.EscrowID, Transfer: p.Transfer, From: p.From, To: p.To, Amount: p.Amount, HashLock: p.HashLock, Deadline: p.Deadline})
		}
		handleErr = err
	case message.KindClaim:
		p, err := message.Decode[SecretPayload](msg)
		if err == nil {
			var escrow *ledger.Escrow
			escrow, err = n.ledger.Claim(p.EscrowID, p.Secret, p.ClaimID, p.Amount)
			if escrow != nil {
				ack.Status = escrow.Status
				ack.ConfirmedTotal = escrow.Claimed
				ack.Remaining = escrow.Remaining()
			}
		}
		handleErr = err
	case message.KindRefund:
		p, err := message.Decode[IDPayload](msg)
		if err == nil {
			var escrow *ledger.Escrow
			escrow, err = n.ledger.Refund(p.EscrowID)
			if escrow != nil {
				ack.Status = escrow.Status
				ack.ConfirmedTotal = escrow.Claimed
				ack.Remaining = escrow.Remaining()
			}
		}
		handleErr = err
	}
	if handleErr != nil {
		ack.Status = "error"
	}

	n.mu.Lock()
	n.handled[msg.ID] = true
	saveErr := store.SaveJSON(n.path, n.handled)
	n.mu.Unlock()
	if saveErr != nil {
		return nil, saveErr
	}
	if handleErr != nil {
		return nil, handleErr
	}
	data, _ := json.Marshal(ack)
	return &message.Message{ID: msg.ID + "-ack", From: n.endpoint, To: msg.From, Kind: message.KindAck, Payload: data}, nil
}
