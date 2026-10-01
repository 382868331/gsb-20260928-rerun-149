package ledger

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"crossledgerlab/internal/clock"
	"crossledgerlab/internal/store"
)

const (
	StatusLocked   = "locked"
	StatusClaimed  = "claimed"
	StatusRefunded = "refunded"
)

var (
	ErrInsufficientFunds = errors.New("insufficient virtual funds")
	ErrAlreadyFinal      = errors.New("escrow already finalized")
	ErrBadSecret         = errors.New("secret does not match hash lock")
	ErrDeadlinePassed    = errors.New("claim deadline passed")
	ErrDeadlineNotPassed = errors.New("refund deadline has not passed")
	ErrMissing           = errors.New("escrow missing")
)

type Account struct {
	Address string `json:"address"`
	Balance uint64 `json:"balance"`
}

type Escrow struct {
	ID        string    `json:"id"`
	Transfer  string    `json:"transfer_id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Amount    uint64    `json:"amount"`
	HashLock  []byte    `json:"hash_lock"`
	Deadline  time.Time `json:"deadline"`
	Status    string    `json:"status"`
	Secret    []byte    `json:"secret,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type LockRequest struct {
	ID       string
	Transfer string
	From     string
	To       string
	Amount   uint64
	HashLock []byte
	Deadline time.Time
}

type Ledger struct {
	name         string
	clock        *clock.SimClock
	accountsPath string
	escrowPath   string
	mu           sync.Mutex
	accounts     map[string]*Account
	escrows      map[string]*Escrow
}

func Open(name, dir string, clock *clock.SimClock) (*Ledger, error) {
	l := &Ledger{
		name:         name,
		clock:        clock,
		accountsPath: filepathJoin(dir, "accounts.json"),
		escrowPath:   filepathJoin(dir, "escrows.json"),
		accounts:     map[string]*Account{},
		escrows:      map[string]*Escrow{},
	}
	if err := store.LoadJSON(l.accountsPath, &l.accounts); err != nil {
		return nil, err
	}
	var escrowList []*Escrow
	if err := store.LoadJSON(l.escrowPath, &escrowList); err != nil {
		return nil, err
	}
	for _, escrow := range escrowList {
		l.escrows[escrow.ID] = escrow
	}
	return l, nil
}

func (l *Ledger) Name() string { return l.name }

func (l *Ledger) Now() time.Time { return l.clock.Now() }

func (l *Ledger) EnsureAccount(address string, balance uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.accounts[address]; !ok {
		l.accounts[address] = &Account{Address: address, Balance: balance}
	}
	return l.saveLocked()
}

// Credit 仅供模拟环境注资虚拟余额，不涉及任何真实资产。
func (l *Ledger) Credit(address string, amount uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	account := l.accounts[address]
	if account == nil {
		return fmt.Errorf("unknown account %s", address)
	}
	account.Balance += amount
	return l.saveLocked()
}

func (l *Ledger) Balance(address string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if account := l.accounts[address]; account != nil {
		return account.Balance
	}
	return 0
}

// LockedBalance 返回某账户当前仍处于 locked 状态、尚未被领取或退回的资金。
// 多笔转移并发时，已锁定的金额不能再次用于另一笔转移。
func (l *Ledger) LockedBalance(address string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var total uint64
	for _, escrow := range l.escrows {
		if escrow.Status == StatusLocked && (escrow.From == address || escrow.To == address) {
			total += escrow.Amount
		}
	}
	return total
}

// AvailableBalance 是账户的可支配余额。锁定在本模型中立即从余额扣减
// 并单独记录在 escrow 中，因此账户当前余额就是尚未被任何锁占用的部分。
func (l *Ledger) AvailableBalance(address string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	account := l.accounts[address]
	if account == nil {
		return 0
	}
	return account.Balance
}

func (l *Ledger) Lock(req LockRequest) (*Escrow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing := l.escrows[req.ID]; existing != nil {
		if existing.Transfer == req.Transfer && existing.From == req.From && existing.To == req.To &&
			existing.Amount == req.Amount && bytes.Equal(existing.HashLock, req.HashLock) &&
			existing.Deadline.Equal(req.Deadline) {
			return cloneEscrow(existing), nil
		}
		return nil, fmt.Errorf("duplicate escrow id with different terms")
	}
	from := l.accounts[req.From]
	if from == nil || from.Balance < req.Amount {
		return nil, ErrInsufficientFunds
	}
	if l.accounts[req.To] == nil {
		return nil, fmt.Errorf("unknown payee %s", req.To)
	}
	if l.clock.Now().After(req.Deadline) {
		return nil, ErrDeadlinePassed
	}
	from.Balance -= req.Amount
	escrow := &Escrow{
		ID:        req.ID,
		Transfer:  req.Transfer,
		From:      req.From,
		To:        req.To,
		Amount:    req.Amount,
		HashLock:  append([]byte(nil), req.HashLock...),
		Deadline:  req.Deadline.UTC(),
		Status:    StatusLocked,
		UpdatedAt: l.clock.Now(),
	}
	l.escrows[escrow.ID] = escrow
	return cloneEscrow(escrow), l.saveLocked()
}

func (l *Ledger) Claim(id string, secret []byte) (*Escrow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	escrow := l.escrows[id]
	if escrow == nil {
		return nil, ErrMissing
	}
	if escrow.Status == StatusClaimed {
		return cloneEscrow(escrow), nil
	}
	if escrow.Status == StatusRefunded {
		return nil, ErrAlreadyFinal
	}
	hash := sha256.Sum256(secret)
	if !bytes.Equal(hash[:], escrow.HashLock) {
		return nil, ErrBadSecret
	}
	if l.clock.Now().After(escrow.Deadline) {
		return nil, ErrDeadlinePassed
	}
	to := l.accounts[escrow.To]
	to.Balance += escrow.Amount
	escrow.Status = StatusClaimed
	escrow.Secret = append([]byte(nil), secret...)
	escrow.UpdatedAt = l.clock.Now()
	return cloneEscrow(escrow), l.saveLocked()
}

func (l *Ledger) Refund(id string) (*Escrow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	escrow := l.escrows[id]
	if escrow == nil {
		return nil, ErrMissing
	}
	if escrow.Status == StatusRefunded {
		return cloneEscrow(escrow), nil
	}
	if escrow.Status == StatusClaimed {
		return nil, ErrAlreadyFinal
	}
	if l.clock.Now().Before(escrow.Deadline) {
		return nil, ErrDeadlineNotPassed
	}
	from := l.accounts[escrow.From]
	from.Balance += escrow.Amount
	escrow.Status = StatusRefunded
	escrow.UpdatedAt = l.clock.Now()
	return cloneEscrow(escrow), l.saveLocked()
}

func (l *Ledger) Status(id string) (*Escrow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	escrow := l.escrows[id]
	if escrow == nil {
		return nil, ErrMissing
	}
	return cloneEscrow(escrow), nil
}

func (l *Ledger) ListByTransfer(transferID string) []*Escrow {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []*Escrow
	for _, escrow := range l.escrows {
		if escrow.Transfer == transferID {
			out = append(out, cloneEscrow(escrow))
		}
	}
	return out
}

// ListLocked 返回该账本上仍处于 locked 状态的全部 escrow，供逐笔对账使用。
func (l *Ledger) ListLocked() []*Escrow {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Escrow, 0)
	for _, escrow := range l.escrows {
		if escrow.Status == StatusLocked {
			out = append(out, cloneEscrow(escrow))
		}
	}
	return out
}

func (l *Ledger) saveLocked() error {
	if err := store.SaveJSON(l.accountsPath, l.accounts); err != nil {
		return err
	}
	escrowList := make([]*Escrow, 0, len(l.escrows))
	for _, escrow := range l.escrows {
		escrowList = append(escrowList, cloneEscrow(escrow))
	}
	return store.SaveJSON(l.escrowPath, escrowList)
}

func cloneEscrow(in *Escrow) *Escrow {
	out := *in
	out.HashLock = append([]byte(nil), in.HashLock...)
	out.Secret = append([]byte(nil), in.Secret...)
	return &out
}

func filepathJoin(parts ...string) string {
	return filepath.Join(parts...)
}
