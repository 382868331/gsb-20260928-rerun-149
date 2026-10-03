package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"sort"
	"strconv"
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
	ID       string          `json:"id"`
	Sender   string          `json:"sender"`
	Receiver string          `json:"receiver"`
	Amount   uint64          `json:"amount"`
	HashLock []byte          `json:"hash_lock"`
	Secret   []byte          `json:"secret,omitempty"`
	Deadline time.Time       `json:"deadline"`
	Stage    string          `json:"stage"`
	Claims   []ClaimDecision `json:"claims,omitempty"`
}

// ClaimDecision 是协调者已决定的一次分次领取；编号与消息 ID 持久化，
// 重启后继续用原编号追查同一决定，而不是以新编号重做可能已发生的效果。
type ClaimDecision struct {
	ClaimID   string `json:"claim_id"`
	Amount    uint64 `json:"amount"`
	MessageID string `json:"message_id"`
	Confirmed bool   `json:"confirmed"`
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
	partialCount := statuses[ledger.StatusPartiallyClaimed]
	refundedAfterPartial := statuses["refunded_after_partial"]
	switch {
	case claimedCount == 2:
		inst.Stage = StageClaimed
	case refundedCount == 2:
		inst.Stage = StageRefunded
	case claimedCount+partialCount+refundedAfterPartial > 0 && refundedCount+refundedAfterPartial > 0:
		inst.Stage = StageMixedExpired
	case lockedCount == 2:
		inst.Stage = StageLocked
	case claimedCount+partialCount > 0:
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

// DecideClaim 决定一次分次领取：先持久化决定（含确定性的 claimID 与消息 ID），再发款。
// 金额以 B 侧实际账本已确认总额为准核对剩余，避免重复决定超发。
func (c *Coordinator) DecideClaim(id string, amount uint64) (*ClaimDecision, error) {
	c.mu.Lock()
	inst := c.instances[id]
	c.mu.Unlock()
	if inst == nil {
		return nil, ledger.ErrMissing
	}
	if amount == 0 {
		return nil, ledger.ErrInvalidAmount
	}
	escrow, err := c.b.Status(escrowID(id, "b"))
	if err != nil {
		return nil, err
	}
	if amount > escrow.Remaining() {
		return nil, ledger.ErrInvalidAmount
	}
	decision := ClaimDecision{
		ClaimID: id + "-claim-" + strconv.Itoa(len(inst.Claims)+1),
		Amount:  amount,
	}
	decision.MessageID = decision.ClaimID + "-msg"
	c.mu.Lock()
	inst.Claims = append(inst.Claims, decision)
	if err := c.saveLocked(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Unlock()
	c.sendClaimMessages(inst, decision)
	return &decision, nil
}

// SyncClaims 重启后追查已决定的领取：用原消息 ID 重发（网络与节点均按 ID 去重），
// 并从实际账本读取已确认总额更新决定状态，不产生第二笔效果。
func (c *Coordinator) SyncClaims(id string) error {
	c.mu.Lock()
	inst := c.instances[id]
	c.mu.Unlock()
	if inst == nil {
		return ledger.ErrMissing
	}
	for _, decision := range inst.Claims {
		c.sendClaimMessages(inst, decision)
	}
	escrow, err := c.b.Status(escrowID(id, "b"))
	if err != nil {
		return err
	}
	confirmed := map[string]bool{}
	for _, record := range escrow.Claims {
		confirmed[record.ClaimID] = true
	}
	c.mu.Lock()
	for i := range inst.Claims {
		inst.Claims[i].Confirmed = confirmed[inst.Claims[i].ClaimID]
	}
	err = c.saveLocked()
	c.mu.Unlock()
	return err
}

func (c *Coordinator) sendClaimMessages(inst *Instance, decision ClaimDecision) {
	for _, side := range []string{"b", "a"} {
		payload := node.SecretPayload{EscrowID: escrowID(inst.ID, side), Secret: inst.Secret, ClaimID: decision.ClaimID, Amount: decision.Amount}
		_, _ = c.net.SendWithID(decision.MessageID+"-"+side, "coordinator", "ledger-"+side, message.KindClaim, payload, 15*time.Millisecond)
	}
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
	copy.Claims = append([]ClaimDecision(nil), inst.Claims...)
	return &copy, nil
}

// SideReport 描述某笔转移在单侧账本上的资金占用与可执行动作。
type SideReport struct {
	EscrowID  string               `json:"escrow_id"`
	Status    string               `json:"status"`
	Locked    uint64               `json:"locked"`
	Confirmed uint64               `json:"confirmed_total"`
	Claims    []ledger.ClaimRecord `json:"claims,omitempty"`
	Refunded  uint64               `json:"refunded_amount,omitempty"`
	Deadline  time.Time            `json:"deadline"`
	Actions   []string             `json:"actions"`
}

// TransferReport 是协调者重新读取两侧实际账本后得到的逐笔对账结果。
type TransferReport struct {
	ID       string     `json:"id"`
	Stage    string     `json:"stage"`
	Amount   uint64     `json:"amount"`
	Deadline time.Time  `json:"deadline"`
	A        SideReport `json:"a"`
	B        SideReport `json:"b"`
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
	report.Confirmed = escrow.ClaimedAmount
	report.Claims = escrow.Claims
	report.Refunded = escrow.RefundedAmount
	now := l.Now()
	switch escrow.Status {
	case ledger.StatusLocked, ledger.StatusPartiallyClaimed:
		report.Locked = escrow.Remaining()
		if now.Before(escrow.Deadline) {
			report.Actions = append(report.Actions, "claim:提交 secret 分次或全额领取，本地截止 "+escrow.Deadline.Format(time.RFC3339))
		} else {
			report.Actions = append(report.Actions, "refund:已过本地截止，可退回剩余 "+strconv.FormatUint(report.Locked, 10))
		}
	case ledger.StatusClaimed, ledger.StatusRefunded:
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
		reports = append(reports, TransferReport{
			ID:       inst.ID,
			Stage:    inst.Stage,
			Amount:   inst.Amount,
			Deadline: inst.Deadline,
			A:        sideReport(c.a, escrowID(id, "a")),
			B:        sideReport(c.b, escrowID(id, "b")),
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
	// 已退回但曾部分领取：同时计入部分结清，避免把整笔误报为完全退回。
	if escrow.Status == ledger.StatusRefunded && escrow.ClaimedAmount > 0 {
		return "refunded_after_partial"
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
