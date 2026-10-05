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
	StageClaimConflict     = "claim_conflict"
)

const (
	ClaimStatePending   = "pending"
	ClaimStateConfirmed = "confirmed"
	ClaimStateRejected  = "rejected"
	ClaimStateConflict  = "conflict"
)

// ResolutionRejectedSideRefunded 表示单侧确认、另一侧明确拒绝的争议已收尾：
// 拒绝是终态事实（截止后不可再领取），拒绝侧仍锁住的争议金额随退款退回，
// 确认侧的已领取事实保留不撤销。
const ResolutionRejectedSideRefunded = "rejected_side_refunded"

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
	State     string `json:"state"`
	Confirmed bool   `json:"confirmed"`
	AState    string `json:"a_state"`
	BState    string `json:"b_state"`
	AReason   string `json:"a_reason,omitempty"`
	BReason   string `json:"b_reason,omitempty"`
	// Resolution 记录争议决定的收尾结果；空表示争议尚未处置。
	Resolution string `json:"resolution,omitempty"`
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
	partialRefundCount := statuses[ledger.StatusPartiallyRefunded]
	refundedAfterPartial := statuses["refunded_after_partial"]
	refundedTotal := refundedCount + refundedAfterPartial
	if err := c.SyncClaims(id); err != nil {
		return nil, err
	}
	hasConflict := false
	for _, decision := range inst.Claims {
		if decision.State == ClaimStateConflict && decision.Resolution == "" {
			hasConflict = true
		}
	}
	switch {
	case hasConflict:
		inst.Stage = StageClaimConflict
	case claimedCount == 2:
		inst.Stage = StageClaimed
	case refundedTotal == 2:
		inst.Stage = StageRefunded
	case claimedCount+partialCount+refundedAfterPartial > 0 && refundedCount+refundedAfterPartial > 0:
		inst.Stage = StageMixedExpired
	case lockedCount+partialCount+partialRefundCount > 0 && refundedCount+partialRefundCount > 0:
		inst.Stage = StagePartiallyRefunded
	case lockedCount == 2:
		inst.Stage = StageLocked
	case claimedCount+partialCount > 0:
		inst.Stage = StagePartiallyClaimed
	case refundedCount+partialRefundCount > 0:
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
	aEscrow, aErr := c.a.Status(escrowID(id, "a"))
	bEscrow, bErr := c.b.Status(escrowID(id, "b"))
	if aErr != nil {
		return nil, aErr
	}
	if bErr != nil {
		return nil, bErr
	}
	reservedA := uint64(0)
	reservedB := uint64(0)
	for _, existing := range inst.Claims {
		if existing.State == ClaimStateConflict && existing.Resolution == "" {
			return nil, ledger.ErrConflictingClaim
		}
		if existing.State == ClaimStatePending {
			if existing.AState != ClaimStateConfirmed {
				reservedA = saturatingAdd(reservedA, existing.Amount)
			}
			if existing.BState != ClaimStateConfirmed {
				reservedB = saturatingAdd(reservedB, existing.Amount)
			}
		}
	}
	remaining := subtractAvailable(aEscrow.Remaining(), reservedA)
	if bAvailable := subtractAvailable(bEscrow.Remaining(), reservedB); bAvailable < remaining {
		remaining = bAvailable
	}
	if amount > remaining {
		return nil, ledger.ErrInvalidAmount
	}
	decision := ClaimDecision{
		ClaimID: id + "-claim-" + strconv.Itoa(len(inst.Claims)+1),
		Amount:  amount,
		State:   ClaimStatePending,
		AState:  ClaimStatePending,
		BState:  ClaimStatePending,
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

func saturatingAdd(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}

func subtractAvailable(remaining, reserved uint64) uint64 {
	if reserved >= remaining {
		return 0
	}
	return remaining - reserved
}

// SyncClaims 用原消息 ID 追查每个领取决定在两侧账本上的事实：
// 已确认、明确拒绝和仍待确认分别落账；重发由网络和节点幂等去重。
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
	aEscrow, aErr := c.a.Status(escrowID(id, "a"))
	bEscrow, bErr := c.b.Status(escrowID(id, "b"))
	c.mu.Lock()
	for i := range inst.Claims {
		aState, aReason := claimFact(aEscrow, aErr, inst.Claims[i].ClaimID)
		bState, bReason := claimFact(bEscrow, bErr, inst.Claims[i].ClaimID)
		inst.Claims[i].AState = aState
		inst.Claims[i].BState = bState
		inst.Claims[i].AReason = aReason
		inst.Claims[i].BReason = bReason
		switch {
		case aState == ClaimStateConfirmed && bState == ClaimStateConfirmed:
			inst.Claims[i].State = ClaimStateConfirmed
		case aState == ClaimStateRejected && bState == ClaimStateRejected:
			inst.Claims[i].State = ClaimStateRejected
		case aState == ClaimStateConfirmed && bState == ClaimStateRejected,
			aState == ClaimStateRejected && bState == ClaimStateConfirmed:
			inst.Claims[i].State = ClaimStateConflict
		default:
			inst.Claims[i].State = ClaimStatePending
		}
		inst.Claims[i].Confirmed = inst.Claims[i].State == ClaimStateConfirmed
	}
	err := c.saveLocked()
	c.mu.Unlock()
	return err
}

func claimFact(escrow *ledger.Escrow, statusErr error, claimID string) (string, string) {
	if statusErr != nil || escrow == nil {
		return ClaimStatePending, ""
	}
	switch escrow.ClaimState(claimID) {
	case ClaimStateConfirmed:
		return ClaimStateConfirmed, ""
	case ClaimStateRejected:
		for _, rejected := range escrow.RejectedClaims {
			if rejected.ClaimID == claimID {
				return ClaimStateRejected, rejected.Reason
			}
		}
		return ClaimStateRejected, "rejected"
	default:
		return ClaimStatePending, ""
	}
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
	if inst == nil {
		return ledger.ErrMissing
	}
	if err := c.SyncClaims(id); err != nil {
		return err
	}
	if !c.clock.Now().After(inst.Deadline) {
		return ledger.ErrDeadlineNotPassed
	}
	aEscrow, aErr := c.a.Status(escrowID(id, "a"))
	bEscrow, bErr := c.b.Status(escrowID(id, "b"))
	for _, decision := range inst.Claims {
		if decision.State == ClaimStatePending &&
			((aErr == nil && decision.AState != ClaimStateConfirmed) || (bErr == nil && decision.BState != ClaimStateConfirmed)) {
			return ledger.ErrPendingClaim
		}
	}
	refund := map[string]uint64{}
	if aErr == nil {
		refund["a"] = aEscrow.Remaining()
	}
	if bErr == nil {
		refund["b"] = bEscrow.Remaining()
	}
	for _, decision := range inst.Claims {
		switch decision.State {
		case ClaimStatePending:
			return ledger.ErrPendingClaim
		case ClaimStateConflict:
			// 一侧确认、另一侧明确拒绝：拒绝是终态事实，不再当作未知；
			// 拒绝侧仍锁住的争议金额已包含在 Remaining 中，随本次退款一并退回，
			// 确认侧的已领取事实保留不撤销。
		case ClaimStateRejected:
			// 双方都明确拒绝时，该金额仍包含在各侧 Remaining 中，应正常退回。
		}
	}
	for _, side := range []string{"a", "b"} {
		if _, ok := refund[side]; !ok {
			continue
		}
		payload := node.IDPayload{EscrowID: escrowID(id, side), Amount: refund[side]}
		messageID := id + "-refund-" + side
		if _, err := c.net.SendWithID(messageID, "coordinator", "ledger-"+side, message.KindRefund, payload, 10*time.Millisecond); err != nil {
			return err
		}
	}
	// 退款消息已发出（确定性消息 ID，重发幂等），把争议决定标记为已收尾，
	// 使原领取决定与其收尾结果都可查询，重开后保留终态和可解释的双方差异。
	c.mu.Lock()
	for i := range inst.Claims {
		if inst.Claims[i].State == ClaimStateConflict && inst.Claims[i].Resolution == "" {
			inst.Claims[i].Resolution = ResolutionRejectedSideRefunded
		}
	}
	err := c.saveLocked()
	c.mu.Unlock()
	return err
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
	EscrowID       string                 `json:"escrow_id"`
	Status         string                 `json:"status"`
	Locked         uint64                 `json:"locked"`
	Confirmed      uint64                 `json:"confirmed_total"`
	Pending        uint64                 `json:"pending_total"`
	Disputed       uint64                 `json:"disputed_total"`
	Refundable     uint64                 `json:"refundable_total"`
	Claims         []ledger.ClaimRecord   `json:"claims,omitempty"`
	RejectedClaims []ledger.RejectedClaim `json:"rejected_claims,omitempty"`
	Refunded       uint64                 `json:"refunded_amount,omitempty"`
	Deadline       time.Time              `json:"deadline"`
	Actions        []string               `json:"actions"`
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

func sideReport(inst *Instance, side string, l *ledger.Ledger, escrowID string) SideReport {
	report := SideReport{EscrowID: escrowID, Status: "missing"}
	escrow, err := l.Status(escrowID)
	if err != nil {
		report.Actions = append(report.Actions, "无锁，无需动作")
		return report
	}
	report.Status = escrow.Status
	report.Deadline = escrow.Deadline
	report.Confirmed = escrow.ClaimedAmount
	report.Locked = escrow.Remaining()
	report.Claims = escrow.Claims
	report.RejectedClaims = escrow.RejectedClaims
	report.Refunded = escrow.RefundedAmount
	sideState := func(decision ClaimDecision) string {
		if side == "a" {
			return decision.AState
		}
		return decision.BState
	}
	for _, decision := range inst.Claims {
		switch decision.State {
		case ClaimStatePending:
			// 只把仍未知的一侧计入待定；已知拒绝是终态，不再当作未知。
			if sideState(decision) == ClaimStatePending {
				report.Pending += decision.Amount
			}
		case ClaimStateConflict:
			if decision.Resolution == "" && sideState(decision) != ClaimStateConfirmed {
				report.Disputed += decision.Amount
			}
		}
	}
	report.Refundable = report.Locked
	if report.Pending+report.Disputed >= report.Refundable {
		report.Refundable = 0
	} else {
		report.Refundable -= report.Pending + report.Disputed
	}
	now := l.Now()
	switch escrow.Status {
	case ledger.StatusLocked, ledger.StatusPartiallyClaimed, ledger.StatusPartiallyRefunded:
		if escrow.Status == ledger.StatusPartiallyRefunded || !now.Before(escrow.Deadline) {
			report.Actions = append(report.Actions, "refund:已过本地截止，可退回 "+strconv.FormatUint(report.Refundable, 10))
			if report.Pending > 0 || report.Disputed > 0 {
				report.Actions = append(report.Actions, "待确认锁定 "+strconv.FormatUint(report.Pending, 10)+"，争议锁定 "+strconv.FormatUint(report.Disputed, 10))
			}
		} else {
			report.Actions = append(report.Actions, "claim:提交 secret 分次或全额领取，本地截止 "+escrow.Deadline.Format(time.RFC3339))
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
			A:        sideReport(inst, "a", c.a, escrowID(id, "a")),
			B:        sideReport(inst, "b", c.b, escrowID(id, "b")),
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
