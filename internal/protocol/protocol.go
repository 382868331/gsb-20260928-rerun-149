package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
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
	StageMixedSettled      = "mixed_settled"
)

// ErrPendingClaims 表示仍有已发出但未确认的领取决定；
// 未知领取不能在等待确认期间被退款，需先 ResumePending 追查原决定。
var ErrPendingClaims = errors.New("unconfirmed claims pending; resume and confirm before refund")

// ClaimIntent 是协调者持久化的一笔分次领取决定。
// 重开或回应丢失后用同一 ID 继续追查，不以新编号重做可能已发生的效果。
type ClaimIntent struct {
	ID             string `json:"id"`
	Amount         uint64 `json:"amount"`
	Confirmed      bool   `json:"confirmed"`
	ConfirmedTotal uint64 `json:"confirmed_total"`
}

type Instance struct {
	ID       string    `json:"id"`
	Sender   string    `json:"sender"`
	Receiver string    `json:"receiver"`
	Amount   uint64    `json:"amount"`
	HashLock []byte    `json:"hash_lock"`
	Secret   []byte    `json:"secret,omitempty"`
	Deadline time.Time `json:"deadline"`
	Stage    string    `json:"stage"`
	Claims   []ClaimIntent `json:"claims,omitempty"`
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
	c.syncConfirmations(inst)
	a, aErr := c.a.Status(escrowID(id, "a"))
	b, bErr := c.b.Status(escrowID(id, "b"))
	stateA, stateB := countState(a, aErr), countState(b, bErr)
	final := func(s string) bool {
		return s == ledger.StatusClaimed || s == ledger.StatusRefunded || s == ledger.StatusMixedSettled
	}
	hasClaim := func(s string) bool {
		return s == ledger.StatusClaimed || s == ledger.StatusPartiallyClaimed || s == ledger.StatusMixedSettled
	}
	hasRefund := func(s string) bool {
		return s == ledger.StatusRefunded || s == ledger.StatusMixedSettled
	}
	switch {
	case stateA == ledger.StatusClaimed && stateB == ledger.StatusClaimed:
		inst.Stage = StageClaimed
	case stateA == ledger.StatusRefunded && stateB == ledger.StatusRefunded:
		inst.Stage = StageRefunded
	case final(stateA) && final(stateB):
		inst.Stage = StageMixedSettled
	case hasClaim(stateA) || hasClaim(stateB):
		inst.Stage = StagePartiallyClaimed
	case hasRefund(stateA) || hasRefund(stateB):
		inst.Stage = StagePartiallyRefunded
	case stateA == ledger.StatusLocked && stateB == ledger.StatusLocked:
		inst.Stage = StageLocked
	case stateA == ledger.StatusLocked || stateB == ledger.StatusLocked:
		inst.Stage = StagePartiallyLocked
	default:
		inst.Stage = StageCreated
	}
	c.mu.Lock()
	err := c.saveLocked()
	c.mu.Unlock()
	return inst, err
}

// syncConfirmations 以实际账本为准核对每笔领取决定：账本上存在同 ID 记录即视为已确认，
// 即使当初的回应丢失也不会重复扣款。
func (c *Coordinator) syncConfirmations(inst *Instance) {
	changed := false
	for i, intent := range inst.Claims {
		if intent.Confirmed {
			continue
		}
		record, err := c.b.ClaimRecord(escrowID(inst.ID, "b"), intent.ID)
		if err != nil {
			continue
		}
		inst.Claims[i].Confirmed = true
		inst.Claims[i].ConfirmedTotal = record.ConfirmedTotal
		changed = true
	}
	if changed {
		c.mu.Lock()
		c.saveLocked()
		c.mu.Unlock()
	}
}

// Claim 一次性领取全部金额（兼容旧接口），等价于一笔全额分次领取。
func (c *Coordinator) Claim(id string) error {
	inst, err := c.Get(id)
	if err != nil {
		return err
	}
	_, err = c.ClaimPartial(id, inst.Amount)
	return err
}

// ClaimPartial 发起一笔分次领取：接收方可分几次领取，未领取余额到期后退回。
// 第一次领取只确认本次金额，不把整笔视为结清。
func (c *Coordinator) ClaimPartial(id string, amount uint64) (string, error) {
	c.mu.Lock()
	inst := c.instances[id]
	c.mu.Unlock()
	if inst == nil {
		return "", ledger.ErrMissing
	}
	if _, err := c.Reconcile(id); err != nil {
		return "", err
	}
	if inst.Stage != StageLocked && inst.Stage != StagePartiallyClaimed {
		return "", nil
	}
	intent := ClaimIntent{ID: fmt.Sprintf("%s-claim-%d", id, len(inst.Claims)+1), Amount: amount}
	c.mu.Lock()
	inst.Claims = append(inst.Claims, intent)
	err := c.saveLocked()
	c.mu.Unlock()
	if err != nil {
		return "", err
	}
	return intent.ID, c.sendClaimMessages(inst, intent)
}

func (c *Coordinator) sendClaimMessages(inst *Instance, intent ClaimIntent) error {
	p := node.SecretPayload{EscrowID: escrowID(inst.ID, "b"), ClaimID: intent.ID, Amount: intent.Amount, Secret: inst.Secret}
	if _, err := c.net.Send("coordinator", "ledger-b", message.KindClaim, p, 15*time.Millisecond); err != nil {
		return err
	}
	p.EscrowID = escrowID(inst.ID, "a")
	_, err := c.net.Send("coordinator", "ledger-a", message.KindClaim, p, 30*time.Millisecond)
	return err
}

// ResumePending 重开后继续追查原领取决定：用持久化的同一 claim ID 重发，
// 账本按 claim ID 幂等，可能已发生的效果不会被重做。
func (c *Coordinator) ResumePending(id string) ([]string, error) {
	c.mu.Lock()
	inst := c.instances[id]
	c.mu.Unlock()
	if inst == nil {
		return nil, ledger.ErrMissing
	}
	if _, err := c.Reconcile(id); err != nil {
		return nil, err
	}
	var resent []string
	for _, intent := range inst.Claims {
		if intent.Confirmed {
			continue
		}
		if err := c.sendClaimMessages(inst, intent); err != nil {
			return resent, err
		}
		resent = append(resent, intent.ID)
	}
	return resent, nil
}

// PendingClaims 返回仍未确认的领取决定 ID。
func (c *Coordinator) PendingClaims(id string) ([]string, error) {
	inst, err := c.Get(id)
	if err != nil {
		return nil, err
	}
	var pending []string
	for _, intent := range inst.Claims {
		if !intent.Confirmed {
			pending = append(pending, intent.ID)
		}
	}
	return pending, nil
}

// RefundExpired 退回过期转移的未领取余额。
// 存在未确认领取决定时拒绝退款（ErrPendingClaims）：未知领取不能一边等待确认一边被退款。
func (c *Coordinator) RefundExpired(id string) error {
	c.mu.Lock()
	inst := c.instances[id]
	c.mu.Unlock()
	if inst == nil || !c.clock.Now().After(inst.Deadline) {
		return ledger.ErrDeadlineNotPassed
	}
	if _, err := c.Reconcile(id); err != nil {
		return err
	}
	for _, intent := range inst.Claims {
		if !intent.Confirmed {
			return ErrPendingClaims
		}
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

// SideReport 描述某笔转移在单侧账本上的资金占用与可执行动作。
type SideReport struct {
	EscrowID  string              `json:"escrow_id"`
	Status    string              `json:"status"`
	Locked    uint64              `json:"locked"`
	Claimed   uint64              `json:"claimed"`
	Refunded  uint64              `json:"refunded"`
	Remaining uint64              `json:"remaining"`
	Claims    []ledger.ClaimRecord `json:"claims,omitempty"`
	Deadline  time.Time           `json:"deadline"`
	Actions   []string            `json:"actions"`
}

// TransferReport 是协调者重新读取两侧实际账本后得到的逐笔对账结果。
type TransferReport struct {
	ID            string     `json:"id"`
	Stage         string     `json:"stage"`
	Amount        uint64     `json:"amount"`
	Deadline      time.Time  `json:"deadline"`
	PendingClaims []string   `json:"pending_claims,omitempty"`
	A             SideReport `json:"a"`
	B             SideReport `json:"b"`
}

func sideReport(l *ledger.Ledger, escrowID string) SideReport {
	report := SideReport{EscrowID: escrowID, Status: "missing"}
	escrow, err := l.Status(escrowID)
	if err != nil {
		report.Actions = append(report.Actions, "无锁，无需动作")
		return report
	}
	report.Status = escrow.Status
	report.Deadline = escrow.Deadline
	report.Claimed = escrow.Claimed
	report.Refunded = escrow.Refunded
	report.Remaining = escrow.Remaining()
	report.Claims = escrow.Claims
	now := l.Now()
	switch escrow.Status {
	case ledger.StatusLocked, ledger.StatusPartiallyClaimed:
		report.Locked = escrow.Remaining()
		if now.Before(escrow.Deadline) {
			report.Actions = append(report.Actions, fmt.Sprintf("claim:提交 secret 分次领取（剩余 %d），本地截止 %s", escrow.Remaining(), escrow.Deadline.Format(time.RFC3339)))
		} else {
			report.Actions = append(report.Actions, fmt.Sprintf("refund:已过本地截止，可退回剩余 %d", escrow.Remaining()))
		}
	case ledger.StatusClaimed, ledger.StatusRefunded, ledger.StatusMixedSettled:
		report.Actions = append(report.Actions, "终态 "+escrow.Status+"，不可再领取或退回")
	}
	return report
}

// Audit 逐笔重新读取两侧实际账本，返回每笔仍被锁住的金额、可采取的动作及其时限。
// 协调者长时间失联后必须调用它，而不是只依据过期的协调日志继续发款。
func (c *Coordinator) Audit() ([]TransferReport, error) {
	c.mu.Lock()
	ids := make([]string, 0, len(c.instances))
	for id := range c.instances {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	sort.Strings(ids)
	reports := make([]TransferReport, 0, len(ids))
	for _, id := range ids {
		inst, err := c.Reconcile(id)
		if err != nil {
			return nil, err
		}
		var pending []string
		for _, intent := range inst.Claims {
			if !intent.Confirmed {
				pending = append(pending, intent.ID)
			}
		}
		reports = append(reports, TransferReport{
			ID:            inst.ID,
			Stage:         inst.Stage,
			Amount:        inst.Amount,
			Deadline:      inst.Deadline,
			PendingClaims: pending,
			A:             sideReport(c.a, escrowID(id, "a")),
			B:             sideReport(c.b, escrowID(id, "b")),
		})
	}
	return reports, nil
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
