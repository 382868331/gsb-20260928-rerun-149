package protocol

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type InstanceStore interface {
	LoadInstances(context.Context) (map[string]Instance, error)
	SaveInstances(context.Context, map[string]Instance) error
}

type Coordinator struct {
	source *Ledger
	dest   *Ledger
	store  InstanceStore
	clock  Clock
	mu     sync.Mutex
}

func NewCoordinator(source, dest *Ledger, store InstanceStore, clock Clock) *Coordinator {
	if clock == nil {
		clock = RealClock{}
	}
	return &Coordinator{source: source, dest: dest, store: store, clock: clock}
}

type CreateTransfer struct {
	ID             string
	Sender         string
	Receiver       string
	Amount         Amount
	Secret         string
	SourceDeadline time.Time
	DestDeadline   time.Time
}

func (c *Coordinator) Create(ctx context.Context, request CreateTransfer) (Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	instances, err := c.store.LoadInstances(ctx)
	if err != nil {
		return Instance{}, err
	}
	if _, exists := instances[request.ID]; exists {
		return Instance{}, ErrAlreadyExists
	}
	if !request.SourceDeadline.After(request.DestDeadline) {
		return Instance{}, fmt.Errorf("source deadline must be later than destination deadline")
	}
	sourceLeg := Transfer{
		ID:         request.ID,
		From:       request.Sender,
		To:         "settlement",
		Amount:     request.Amount,
		SecretHash: HashSecret(request.Secret),
	}
	destLeg := Transfer{
		ID:         request.ID,
		From:       "settlement",
		To:         request.Receiver,
		Amount:     request.Amount,
		SecretHash: HashSecret(request.Secret),
	}
	destTransfer := destLeg
	destTransfer.LockUntil = request.DestDeadline
	if _, err := c.dest.Prepare(ctx, destTransfer); err != nil {
		return Instance{}, fmt.Errorf("prepare destination: %w", err)
	}
	sourceTransfer := sourceLeg
	sourceTransfer.LockUntil = request.SourceDeadline
	if _, err := c.source.Prepare(ctx, sourceTransfer); err != nil {
		return Instance{}, fmt.Errorf("prepare source: %w", err)
	}
	instance := Instance{
		ID:             request.ID,
		SourceLedger:   c.source.Name(),
		DestLedger:     c.dest.Name(),
		Sender:         request.Sender,
		Receiver:       request.Receiver,
		Amount:         request.Amount,
		SecretHash:     HashSecret(request.Secret),
		SourceDeadline: request.SourceDeadline,
		DestDeadline:   request.DestDeadline,
		Status:         StatusPrepared,
		UpdatedAt:      c.clock.Now(),
	}
	instances[request.ID] = instance
	return instance, c.store.SaveInstances(ctx, instances)
}

func (c *Coordinator) Claim(ctx context.Context, id, secret string, now time.Time) (Instance, Transfer, Transfer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	instances, err := c.store.LoadInstances(ctx)
	if err != nil {
		return Instance{}, Transfer{}, Transfer{}, err
	}
	instance, ok := instances[id]
	if !ok {
		return Instance{}, Transfer{}, Transfer{}, ErrNotFound
	}
	destTransfer, err := c.dest.Claim(ctx, id, secret)
	if err != nil {
		return instance, Transfer{}, Transfer{}, err
	}
	sourceTransfer, err := c.source.Claim(ctx, id, secret)
	if err != nil {
		instance.Status = StatusRecovering
		instance.UpdatedAt = now
		instances[id] = instance
		_ = c.store.SaveInstances(ctx, instances)
		return instance, sourceTransfer, destTransfer, fmt.Errorf("settle source after destination claim: %w", err)
	}
	instance.Status = StatusCommitted
	instance.UpdatedAt = now
	instances[id] = instance
	if err := c.store.SaveInstances(ctx, instances); err != nil {
		return instance, sourceTransfer, destTransfer, err
	}
	return instance, sourceTransfer, destTransfer, nil
}

func (c *Coordinator) Refund(ctx context.Context, id string, now time.Time) (Instance, Transfer, Transfer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	instances, err := c.store.LoadInstances(ctx)
	if err != nil {
		return Instance{}, Transfer{}, Transfer{}, err
	}
	instance, ok := instances[id]
	if !ok {
		return Instance{}, Transfer{}, Transfer{}, ErrNotFound
	}
	destTransfer, err := c.dest.GetTransfer(ctx, id)
	if err != nil {
		return instance, Transfer{}, Transfer{}, err
	}
	sourceTransfer, err := c.source.GetTransfer(ctx, id)
	if err != nil {
		return instance, sourceTransfer, destTransfer, err
	}
	if destTransfer.Status == TransferSettled || sourceTransfer.Status == TransferSettled {
		return instance, sourceTransfer, destTransfer, ErrConflict
	}
	refundable := true
	if sourceTransfer.Status == TransferHeld && !now.After(sourceTransfer.LockUntil) {
		refundable = false
	}
	if destTransfer.Status == TransferHeld && !now.After(destTransfer.LockUntil) {
		refundable = false
	}
	if !refundable {
		return instance, sourceTransfer, destTransfer, ErrTimeout
	}
	if destTransfer.Status == TransferHeld {
		destTransfer, err = c.dest.Refund(ctx, id)
		if err != nil {
			return instance, sourceTransfer, destTransfer, err
		}
	}
	if sourceTransfer.Status == TransferHeld {
		sourceTransfer, err = c.source.Refund(ctx, id)
		if err != nil {
			instance.Status = StatusRecovering
			instance.UpdatedAt = now
			instances[id] = instance
			_ = c.store.SaveInstances(ctx, instances)
			return instance, sourceTransfer, destTransfer, err
		}
	}
	instance.Status = StatusRefunded
	instance.UpdatedAt = now
	instances[id] = instance
	return instance, sourceTransfer, destTransfer, c.store.SaveInstances(ctx, instances)
}

func (c *Coordinator) RecoverTransfer(ctx context.Context, id string, now time.Time) (Instance, Transfer, Transfer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recoverLocked(ctx, id, now)
}

func (c *Coordinator) Observe(ctx context.Context, id string) (ObservedState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	instances, err := c.store.LoadInstances(ctx)
	if err != nil {
		return ObservedState{}, err
	}
	instance, ok := instances[id]
	if !ok {
		return ObservedState{}, ErrNotFound
	}
	sourceTransfer, err := c.source.GetTransfer(ctx, id)
	if err != nil {
		return ObservedState{}, err
	}
	destTransfer, err := c.dest.GetTransfer(ctx, id)
	if err != nil {
		return ObservedState{}, err
	}
	return ObservedState{Instance: instance, Source: sourceTransfer, Dest: destTransfer}, nil
}

func (c *Coordinator) recoverLocked(ctx context.Context, id string, now time.Time) (Instance, Transfer, Transfer, error) {
	instances, err := c.store.LoadInstances(ctx)
	if err != nil {
		return Instance{}, Transfer{}, Transfer{}, err
	}
	instance, ok := instances[id]
	if !ok {
		return Instance{}, Transfer{}, Transfer{}, ErrNotFound
	}
	sourceTransfer, err := c.source.GetTransfer(ctx, id)
	if err != nil {
		return instance, sourceTransfer, Transfer{}, err
	}
	destTransfer, err := c.dest.GetTransfer(ctx, id)
	if err != nil {
		return instance, sourceTransfer, destTransfer, err
	}
	if destTransfer.Status == TransferSettled {
		if sourceTransfer.Status == TransferHeld && HashSecret(destTransfer.Secret) == sourceTransfer.SecretHash {
			sourceTransfer, err = c.source.Claim(ctx, id, destTransfer.Secret)
			if err != nil && err != ErrLockExpired {
				return instance, sourceTransfer, destTransfer, err
			}
		}
		if sourceTransfer.Status == TransferSettled {
			instance.Status = StatusCommitted
		} else {
			instance.Status = StatusRecovering
		}
	} else if sourceTransfer.Status == TransferSettled {
		instance.Status = StatusRecovering
	} else if sourceTransfer.Status == TransferRefunded && destTransfer.Status == TransferRefunded {
		instance.Status = StatusRefunded
	} else if now.After(sourceTransfer.LockUntil) && now.After(destTransfer.LockUntil) {
		if destTransfer.Status == TransferHeld {
			destTransfer, err = c.dest.Refund(ctx, id)
			if err != nil {
				return instance, sourceTransfer, destTransfer, err
			}
		}
		if sourceTransfer.Status == TransferHeld {
			sourceTransfer, err = c.source.Refund(ctx, id)
			if err != nil {
				return instance, sourceTransfer, destTransfer, err
			}
		}
		instance.Status = StatusRefunded
	} else {
		instance.Status = StatusPrepared
	}
	instance.UpdatedAt = now
	instances[id] = instance
	return instance, sourceTransfer, destTransfer, c.store.SaveInstances(ctx, instances)
}
