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
)

var (
	ErrInsufficientFunds = errors.New("insufficient virtual funds")
	ErrAlreadyFinal      = errors.New("escrow already finalized")
	ErrBadSecret         = errors.New("secret does not match hash lock")
	ErrDeadlinePassed    = errors.New("claim deadline passed")
	ErrDeadlineNotPassed = errors.New("refund deadline has not passed")
	ErrMissing           = errors.New("escrow missing")
	ErrInvalidAmount     = errors.New("claim amount exceeds remaining locked balance")
)

type Account struct {
	Address string `json:"address"`
	Balance uint64 `json:"balance"`
}

type Escrow struct {
	ID             string        `json:"id"`
	Transfer       string        `json:"transfer_id"`
	From           string        `json:"from"`
	To             string        `json:"to"`
	Amount         uint64        `json:"amount"`
	HashLock       []byte        `json:"hash_lock"`
	Deadline       time.Time     `json:"deadline"`
	Status         string        `json:"status"`
	Secret         []byte        `json:"secret,omitempty"`
	ClaimedAmount  uint64        `json:"claimed_amount"`
	Claims         []ClaimRecord `json:"claims,omitempty"`
	RefundedAmount uint64        `json:"refunded_amount,omitempty"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

// ClaimRecord 是一笔可追溯的分次领取记录：本次金额、已确认总额、仍不确定的剩余部分。
type ClaimRecord struct {
	ClaimID   string    `json:"claim_id"`
	Amount    uint64    `json:"amount"`
	Confirmed uint64    `json:"confirmed_total"`
	Remaining uint64    `json:"remaining"`
	Time      time.Time `json:"time"`
}

// Remaining 返回仍锁定、尚未领取也未退回的金额。
func (e *Escrow) Remaining() uint64 {
	if e.Status == StatusRefunded || e.Status == StatusClaimed {
		return 0
	}
	return e.Amount - e.ClaimedAmount
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

func (l *Ledger) Claim(id string, secret []byte) (*Escrow, error) {
	escrow, _, err := l.ClaimPartial(id, secret, "full", 0)
	return escrow, err
}

// ClaimPartial 按 claimID 幂等地领取一部分金额；amount 为 0 表示领取全部剩余。
// 相同 claimID 的重复请求或丢失后重发的回应直接返回首次记录，不重复加款。
// 决定边界：仅在严格早于本地截止时间时接受领取，且在账本互斥锁内原子记账，
// 因此已接受的领取不会一边等待确认一边又被退款。
func (l *Ledger) ClaimPartial(id string, secret []byte, claimID string, amount uint64) (*Escrow, *ClaimRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	escrow := l.escrows[id]
	if escrow == nil {
		return nil, nil, ErrMissing
	}
	for i := range escrow.Claims {
		if escrow.Claims[i].ClaimID == claimID {
			record := escrow.Claims[i]
			return cloneEscrow(escrow), &record, nil
		}
	}
	if escrow.Status == StatusRefunded || escrow.Status == StatusClaimed {
		return nil, nil, ErrAlreadyFinal
	}
	hash := sha256.Sum256(secret)
	if !bytes.Equal(hash[:], escrow.HashLock) {
		return nil, nil, ErrBadSecret
	}
	if !l.clock.Now().Before(escrow.Deadline) {
		return nil, nil, ErrDeadlinePassed
	}
	remaining := escrow.Amount - escrow.ClaimedAmount
	if amount == 0 {
		amount = remaining
	}
	if amount > remaining {
		return nil, nil, ErrInvalidAmount
	}
	to := l.accounts[escrow.To]
	to.Balance += amount
	escrow.ClaimedAmount += amount
	record := ClaimRecord{ClaimID: claimID, Amount: amount, Confirmed: escrow.ClaimedAmount, Remaining: escrow.Amount - escrow.ClaimedAmount, Time: l.clock.Now()}
	escrow.Claims = append(escrow.Claims, record)
	if escrow.ClaimedAmount == escrow.Amount {
		escrow.Status = StatusClaimed
	} else {
		escrow.Status = StatusPartiallyClaimed
	}
	escrow.Secret = append([]byte(nil), secret...)
	escrow.UpdatedAt = l.clock.Now()
	return cloneEscrow(escrow), &record, l.saveLocked()
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
	remaining := escrow.Amount - escrow.ClaimedAmount
	from := l.accounts[escrow.From]
	from.Balance += remaining
	escrow.RefundedAmount = remaining
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

// TotalBalance 返回所有账户余额之和。
func (l *Ledger) TotalBalance() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var total uint64
	for _, account := range l.accounts {
		total += account.Balance
	}
	return total
}

// LockedTotal 返回所有未终结 escrow 仍锁住的金额之和（余额 + 锁定应守恒）。
func (l *Ledger) LockedTotal() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var total uint64
	for _, escrow := range l.escrows {
		total += escrow.Remaining()
	}
	return total
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
