package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"sync"
	"time"

	"crossledgerlab/internal/clock"
	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/message"
	"crossledgerlab/internal/node"
	"crossledgerlab/internal/store"
)

const (
	StageCreated           = "created"
	StagePartiallyLocked   = "partially_locked"
	StageLocked            = "locked"
	StagePartiallyClaimed  = "partially_claimed"
	StageClaimed           = "claimed"
	StagePartiallyRefunded = "partially_refunded"
	StageRefunded          = "refunded"
	StageMixedExpired      = "mixed_expired"
)

type Instance struct {
	ID       string    `json:"id"`
	Sender   string    `json:"sender"`
	Receiver string    `json:"receiver"`
	Amount   uint64    `json:"amount"`
	HashLock []byte    `json:"hash_lock"`
	Secret   []byte    `json:"secret,omitempty"`
	Deadline time.Time `json:"deadline"`
	Stage    string    `json:"stage"`
}

type Coordinator struct {
	mu        sync.Mutex
	path      string
	clock     *clock.SimClock
	net       *message.Network
	a         *ledger.Ledger
	b         *ledger.Ledger
	offsetA   time.Duration
	offsetB   time.Duration
	instances map[string]*Instance
}

func New(dir string, c *clock.SimClock, net *message.Network, a, b *ledger.Ledger) *Coordinator {
	return &Coordinator{path: filepath.Join(dir, "protocol", "instances.json"), clock: c, net: net, a: a, b: b, instances: map[string]*Instance{}}
}

func (c *Coordinator) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return store.LoadJSON(c.path, &c.instances)
}

func (c *Coordinator) Begin(id, sender, receiver string, amount uint64, duration time.Duration) (*Instance, error) {
	secret, hash, err := newSecret()
	if err != nil {
		return nil, err
	}
	inst := &Instance{ID: id, Sender: sender, Receiver: receiver, Amount: amount, HashLock: hash, Secret: secret, Deadline: c.clock.Now().Add(duration).UTC(), Stage: StageCreated}
	c.mu.Lock()
	c.instances[id] = inst
	err = c.saveLocked()
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	lock := node.LockPayload{EscrowID: escrowID(id, "a"), Transfer: id, From: sender, To: "treasury-a", Amount: amount, HashLock: hash, Deadline: inst.Deadline.Add(c.offsetA)}
	if _, err := c.net.Send("coordinator", "ledger-a", message.KindLock, lock, 10*time.Millisecond); err != nil {
		return nil, err
	}
	lock.EscrowID = escrowID(id, "b")
	lock.From = "treasury-b"
	lock.To = receiver
	lock.Deadline = inst.Deadline.Add(c.offsetB)
	_, err = c.net.Send("coordinator", "ledger-b", message.KindLock, lock, 25*time.Millisecond)
	return inst, err
}

func (c *Coordinator) Reconcile(id string) (*Instance, error) {
	c.mu.Lock()
	inst := c.instances[id]
	c.mu.Unlock()
	if inst == nil {
		return nil, ledger.ErrMissing
	}
	a, aErr := c.a.Status(escrowID(id, "a"))
	b, bErr := c.b.Status(escrowID(id, "b"))
	statuses := map[string]int{}
	statuses[countState(a, aErr)]++
	statuses[countState(b, bErr)]++
	lockedCount := statuses[ledger.StatusLocked]
	claimedCount := statuses[ledger.StatusClaimed]
	refundedCount := statuses[ledger.StatusRefunded]
	switch {
	case claimedCount == 2:
		inst.Stage = StageClaimed
	case refundedCount == 2:
		inst.Stage = StageRefunded
	case claimedCount > 0 && refundedCount > 0:
		inst.Stage = StageMixedExpired
	case lockedCount == 2:
		inst.Stage = StageLocked
	case claimedCount > 0:
		inst.Stage = StagePartiallyClaimed
	case refundedCount > 0:
		inst.Stage = StagePartiallyRefunded
	case lockedCount > 0:
		inst.Stage = StagePartiallyLocked
	default:
		inst.Stage = StageCreated
	}
	c.mu.Lock()
	err := c.saveLocked()
	c.mu.Unlock()
	return inst, err
}

func (c *Coordinator) Claim(id string) error {
	c.mu.Lock()
	inst := c.instances[id]
	c.mu.Unlock()
	if inst == nil {
		return ledger.ErrMissing
	}
	if _, err := c.Reconcile(id); err != nil {
		return err
	}
	if inst.Stage != StageLocked {
		return nil
	}
	p := node.SecretPayload{EscrowID: escrowID(id, "b"), Secret: inst.Secret}
	if _, err := c.net.Send("coordinator", "ledger-b", message.KindClaim, p, 15*time.Millisecond); err != nil {
		return err
	}
	p.EscrowID = escrowID(id, "a")
	_, err := c.net.Send("coordinator", "ledger-a", message.KindClaim, p, 30*time.Millisecond)
	return err
}

func (c *Coordinator) RefundExpired(id string) error {
	c.mu.Lock()
	inst := c.instances[id]
	c.mu.Unlock()
	if inst == nil || !c.clock.Now().After(inst.Deadline) {
		return ledger.ErrDeadlineNotPassed
	}
	for _, side := range []string{"a", "b"} {
		if _, err := c.net.Send("coordinator", "ledger-"+side, message.KindRefund, node.IDPayload{EscrowID: escrowID(id, side)}, 10*time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) Get(id string) (*Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	inst := c.instances[id]
	if inst == nil {
		return nil, ledger.ErrMissing
	}
	copy := *inst
	return &copy, nil
}

func (c *Coordinator) SetClockOffsets(offsetA, offsetB time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offsetA = offsetA
	c.offsetB = offsetB
}

func (c *Coordinator) saveLocked() error {
	return store.SaveJSON(c.path, c.instances)
}

func EscrowID(id, side string) string { return escrowID(id, side) }

func escrowID(id, side string) string { return id + "-" + side }

func countState(escrow *ledger.Escrow, err error) string {
	if err != nil || escrow == nil {
		return "missing"
	}
	return escrow.Status
}

func newSecret() ([]byte, []byte, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, nil, err
	}
	hash := sha256Sum(secret)
	return secret, hash, nil
}

func HashSecret(secret []byte) []byte { return sha256Sum(secret) }

func sha256Sum(secret []byte) []byte {
	h := sha256.Sum256(secret)
	return h[:]
}

func DecodeHexSecret(s string) ([]byte, error) { return hex.DecodeString(s) }
