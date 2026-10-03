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
	StatusLocked           = "locked"
	StatusPartiallyClaimed = "partially_claimed"
	StatusClaimed          = "claimed"
	StatusRefunded         = "refunded"
	StatusMixedSettled     = "mixed_settled"
)

var (
	ErrInsufficientFunds = errors.New("insufficient virtual funds")
	ErrAlreadyFinal      = errors.New("escrow already finalized")
	ErrBadSecret         = errors.New("secret does not match hash lock")
	ErrDeadlinePassed    = errors.New("claim deadline passed")
	ErrDeadlineNotPassed = errors.New("refund deadline has not passed")
	ErrMissing           = errors.New("escrow missing")
	ErrBadAmount         = errors.New("claim amount must be positive")
	ErrExceedsRemaining  = errors.New("claim amount exceeds remaining escrow balance")
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
	Claimed   uint64        `json:"claimed"`
	Refunded  uint64        `json:"refunded"`
	Claims    []ClaimRecord `json:"claims,omitempty"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// ClaimRecord 是一笔已确认的分次领取记录：本次金额、累计已确认总额、领取后仍不确定的剩余部分。
type ClaimRecord struct {
	ID             string    `json:"id"`
	Amount         uint64    `json:"amount"`
	ConfirmedTotal uint64    `json:"confirmed_total"`
	Remaining      uint64    `json:"remaining"`
	RecordedAt     time.Time `json:"recorded_at"`
}

// Remaining 返回 escrow 中尚未领取也未退回的金额。
func (e *Escrow) Remaining() uint64 {
	return e.Amount - e.Claimed - e.Refunded
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

func (l *Ledger) Balance(address string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if account := l.accounts[address]; account != nil {
		return account.Balance
	}
	return 0
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

// Claim 记录一笔分次领取。claimID 是领取决定的稳定标识：
// 重复请求或回应丢失后的重传携带同一 claimID，直接返回已确认记录，不会重复加款。
func (l *Ledger) Claim(id string, secret []byte, claimID string, amount uint64) (*Escrow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	escrow := l.escrows[id]
	if escrow == nil {
		return nil, ErrMissing
	}
	for _, record := range escrow.Claims {
		if record.ID == claimID {
			return cloneEscrow(escrow), nil
		}
	}
	if escrow.Remaining() == 0 {
		return nil, ErrAlreadyFinal
	}
	hash := sha256.Sum256(secret)
	if !bytes.Equal(hash[:], escrow.HashLock) {
		return nil, ErrBadSecret
	}
	if l.clock.Now().After(escrow.Deadline) {
		return nil, ErrDeadlinePassed
	}
	if amount == 0 {
		return nil, ErrBadAmount
	}
	if amount > escrow.Remaining() {
		return nil, ErrExceedsRemaining
	}
	to := l.accounts[escrow.To]
	to.Balance += amount
	escrow.Claimed += amount
	escrow.Claims = append(escrow.Claims, ClaimRecord{
		ID:             claimID,
		Amount:         amount,
		ConfirmedTotal: escrow.Claimed,
		Remaining:      escrow.Remaining(),
		RecordedAt:     l.clock.Now(),
	})
	escrow.Secret = append([]byte(nil), secret...)
	escrow.UpdatedAt = l.clock.Now()
	l.updateStatusLocked(escrow)
	return cloneEscrow(escrow), l.saveLocked()
}

// ClaimRecord 返回某笔已确认领取的记录，供协调者重开后核对原决定。
func (l *Ledger) ClaimRecord(id, claimID string) (*ClaimRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	escrow := l.escrows[id]
	if escrow == nil {
		return nil, ErrMissing
	}
	for _, record := range escrow.Claims {
		if record.ID == claimID {
			copy := record
			return &copy, nil
		}
	}
	return nil, ErrMissing
}

// Refund 在截止时间之后把剩余未领取余额退回付款方。
// 决定边界：领取在 now <= deadline 有效，退回仅在 now > deadline 有效，二者互斥。
// 已部分领取的 escrow 只退回剩余部分；重复退回请求幂等，不会二次加款。
func (l *Ledger) Refund(id string) (*Escrow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	escrow := l.escrows[id]
	if escrow == nil {
		return nil, ErrMissing
	}
	if escrow.Remaining() == 0 {
		if escrow.Claimed == escrow.Amount {
			return nil, ErrAlreadyFinal
		}
		return cloneEscrow(escrow), nil
	}
	if !l.clock.Now().After(escrow.Deadline) {
		return nil, ErrDeadlineNotPassed
	}
	from := l.accounts[escrow.From]
	from.Balance += escrow.Remaining()
	escrow.Refunded += escrow.Remaining()
	escrow.UpdatedAt = l.clock.Now()
	l.updateStatusLocked(escrow)
	return cloneEscrow(escrow), l.saveLocked()
}

func (l *Ledger) updateStatusLocked(escrow *Escrow) {
	switch {
	case escrow.Claimed == escrow.Amount:
		escrow.Status = StatusClaimed
	case escrow.Refunded == escrow.Amount:
		escrow.Status = StatusRefunded
	case escrow.Claimed+escrow.Refunded == escrow.Amount:
		escrow.Status = StatusMixedSettled
	case escrow.Claimed > 0:
		escrow.Status = StatusPartiallyClaimed
	default:
		escrow.Status = StatusLocked
	}
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
	out.Claims = append([]ClaimRecord(nil), in.Claims...)
	return &out
}

func filepathJoin(parts ...string) string {
	return filepath.Join(parts...)
}
