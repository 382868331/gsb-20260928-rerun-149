package protocol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now().UTC() }

type StaticClock time.Time

func (c StaticClock) Now() time.Time { return time.Time(c) }

type MutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewMutableClock(now time.Time) *MutableClock { return &MutableClock{now: now} }

func (c *MutableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *MutableClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

type LedgerStore interface {
	LoadAccounts(context.Context) (map[string]Account, error)
	SaveAccounts(context.Context, map[string]Account) error
	LoadTransfers(context.Context) (map[string]Transfer, error)
	SaveTransfers(context.Context, map[string]Transfer) error
}

type Ledger struct {
	name  string
	store LedgerStore
	clock Clock
	mu    sync.Mutex
}

func NewLedger(name string, store LedgerStore, clock Clock) *Ledger {
	if clock == nil {
		clock = RealClock{}
	}
	return &Ledger{name: name, store: store, clock: clock}
}

func (l *Ledger) Name() string { return l.name }

func (l *Ledger) Store() LedgerStore { return l.store }

func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func (l *Ledger) Bootstrap(ctx context.Context, accounts ...Account) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	current, err := l.store.LoadAccounts(ctx)
	if err != nil {
		return err
	}
	if current == nil {
		current = map[string]Account{}
	}
	for _, account := range accounts {
		if _, exists := current[account.Name]; exists {
			continue
		}
		account.Held = 0
		current[account.Name] = account
	}
	return l.store.SaveAccounts(ctx, current)
}

func (l *Ledger) Snapshot(ctx context.Context) (map[string]Account, map[string]Transfer, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	accounts, err := l.store.LoadAccounts(ctx)
	if err != nil {
		return nil, nil, err
	}
	transfers, err := l.store.LoadTransfers(ctx)
	if err != nil {
		return nil, nil, err
	}
	return accounts, transfers, nil
}

func (l *Ledger) GetTransfer(ctx context.Context, id string) (Transfer, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	transfers, err := l.store.LoadTransfers(ctx)
	if err != nil {
		return Transfer{}, err
	}
	transfer, ok := transfers[id]
	if !ok {
		return Transfer{}, ErrNotFound
	}
	return transfer, nil
}

func (l *Ledger) Prepare(ctx context.Context, transfer Transfer) (Transfer, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	accounts, err := l.store.LoadAccounts(ctx)
	if err != nil {
		return Transfer{}, err
	}
	transfers, err := l.store.LoadTransfers(ctx)
	if err != nil {
		return Transfer{}, err
	}
	if _, exists := transfers[transfer.ID]; exists {
		return transfers[transfer.ID], ErrAlreadyExists
	}
	account, ok := accounts[transfer.From]
	if !ok {
		return Transfer{}, ErrNotFound
	}
	if account.Balance-account.Held < transfer.Amount {
		return Transfer{}, ErrInsufficient
	}
	account.Held += transfer.Amount
	accounts[transfer.From] = account
	transfer.Ledger = l.name
	transfer.Status = TransferHeld
	transfer.PreparedAt = l.clock.Now()
	transfers[transfer.ID] = transfer
	if err := l.store.SaveAccounts(ctx, accounts); err != nil {
		return Transfer{}, err
	}
	if err := l.store.SaveTransfers(ctx, transfers); err != nil {
		return Transfer{}, err
	}
	return transfer, nil
}

func (l *Ledger) Claim(ctx context.Context, id, secret string) (Transfer, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	accounts, transfers, err := l.load(ctx)
	if err != nil {
		return Transfer{}, err
	}
	transfer, ok := transfers[id]
	if !ok {
		return Transfer{}, ErrNotFound
	}
	now := l.clock.Now()
	if transfer.Status == TransferSettled && transfer.Secret == secret {
		return transfer, nil
	}
	if transfer.Status != TransferHeld {
		return transfer, ErrInvalidState
	}
	if now.After(transfer.LockUntil) {
		return transfer, ErrLockExpired
	}
	if HashSecret(secret) != transfer.SecretHash {
		return transfer, ErrBadSecret
	}
	from := accounts[transfer.From]
	to := accounts[transfer.To]
	from.Held -= transfer.Amount
	from.Balance -= transfer.Amount
	to.Balance += transfer.Amount
	accounts[transfer.From] = from
	accounts[transfer.To] = to
	transfer.Status = TransferSettled
	transfer.Secret = secret
	transfer.SettledAt = now
	transfers[id] = transfer
	if err := l.save(ctx, accounts, transfers); err != nil {
		return Transfer{}, err
	}
	return transfer, nil
}

func (l *Ledger) Refund(ctx context.Context, id string) (Transfer, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	accounts, transfers, err := l.load(ctx)
	if err != nil {
		return Transfer{}, err
	}
	transfer, ok := transfers[id]
	if !ok {
		return Transfer{}, ErrNotFound
	}
	if transfer.Status == TransferRefunded {
		return transfer, nil
	}
	if transfer.Status != TransferHeld {
		return transfer, ErrInvalidState
	}
	now := l.clock.Now()
	if !now.After(transfer.LockUntil) {
		return transfer, ErrTimeout
	}
	from := accounts[transfer.From]
	from.Held -= transfer.Amount
	accounts[transfer.From] = from
	transfer.Status = TransferRefunded
	transfer.RefundedAt = now
	transfers[id] = transfer
	if err := l.save(ctx, accounts, transfers); err != nil {
		return Transfer{}, err
	}
	return transfer, nil
}

func (l *Ledger) load(ctx context.Context) (map[string]Account, map[string]Transfer, error) {
	accounts, err := l.store.LoadAccounts(ctx)
	if err != nil {
		return nil, nil, err
	}
	transfers, err := l.store.LoadTransfers(ctx)
	if err != nil {
		return nil, nil, err
	}
	return accounts, transfers, nil
}

func (l *Ledger) save(ctx context.Context, accounts map[string]Account, transfers map[string]Transfer) error {
	if err := l.store.SaveAccounts(ctx, accounts); err != nil {
		return err
	}
	return l.store.SaveTransfers(ctx, transfers)
}
