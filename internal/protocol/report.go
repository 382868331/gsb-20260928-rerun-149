package protocol

import (
	"time"

	"crossledgerlab/internal/ledger"
)

const (
	ActionClaim       = "claim"
	ActionRefund      = "refund"
	ActionWait        = "wait_refund_window"
	ActionNoneFinal   = "none_final"
	ActionRecoverLock = "resend_lock"
)

// SideView 是从某一侧真实账本重新读取到的单笔 escrow 视图。
type SideView struct {
	Side      string    `json:"side"`
	Present   bool      `json:"present"`
	Status    string    `json:"status,omitempty"`
	Amount    uint64    `json:"amount"`
	Deadline  time.Time `json:"deadline,omitempty"`
	LocalNow  time.Time `json:"local_now"`
	Locked    bool      `json:"locked"`
	Remaining string    `json:"remaining_until_deadline"`
	Overdue   string    `json:"overdue_since_deadline"`
}

// AllowedAction 描述某一侧当前可采取的动作及其时间窗。
type AllowedAction struct {
	Side       string    `json:"side"`
	Action     string    `json:"action"`
	Allowed    bool      `json:"allowed"`
	ValidFrom  time.Time `json:"valid_from,omitempty"`
	ValidUntil time.Time `json:"valid_until,omitempty"`
	Note       string    `json:"note"`
}

// TransferReport 是单笔转移的对账结果：重新读取两侧账本后得出。
type TransferReport struct {
	TransferID string          `json:"transfer_id"`
	Sender     string          `json:"sender"`
	Receiver   string          `json:"receiver"`
	Amount     uint64          `json:"amount"`
	Stage      string          `json:"stage"`
	Sides      []SideView      `json:"sides"`
	Actions    []AllowedAction `json:"actions"`
	LockedA    uint64          `json:"locked_amount_a"`
	LockedB    uint64          `json:"locked_amount_b"`
}

// LedgerLock 汇总某一账本上仍被锁住的资金。
type LedgerLock struct {
	Ledger    string `json:"ledger"`
	Account   string `json:"account"`
	Locked    uint64 `json:"locked"`
	Available uint64 `json:"available"`
}

// ReconcileReport 强制重新读取两套账本，输出每笔转移的锁住金额、
// 可采取的动作与时限，以及两侧账本当前锁定资金汇总。
// 协调者即使长时间失联，也不能只依据本地过期日志继续发款。
func (c *Coordinator) ReconcileReport(id string) (*TransferReport, []LedgerLock, error) {
	inst, err := c.Reconcile(id)
	if err != nil {
		return nil, nil, err
	}
	report := &TransferReport{
		TransferID: id,
		Sender:     inst.Sender,
		Receiver:   inst.Receiver,
		Amount:     inst.Amount,
		Stage:      inst.Stage,
	}
	sideLedgers := []struct {
		side   string
		ledger *ledger.Ledger
	}{
		{"a", c.a}, {"b", c.b},
	}
	for _, sl := range sideLedgers {
		view, action := readSide(sl.side, sl.ledger, escrowID(id, sl.side))
		report.Sides = append(report.Sides, view)
		report.Actions = append(report.Actions, action)
		if view.Locked && view.Side == "a" {
			report.LockedA += view.Amount
		}
		if view.Locked && view.Side == "b" {
			report.LockedB += view.Amount
		}
	}
	summary := []LedgerLock{
		{Ledger: "A", Account: inst.Sender, Locked: c.a.LockedBalance(inst.Sender), Available: c.a.AvailableBalance(inst.Sender)},
		{Ledger: "B", Account: TreasuryB, Locked: c.b.LockedBalance(TreasuryB), Available: c.b.AvailableBalance(TreasuryB)},
	}
	return report, summary, nil
}

// AllTransfers 按协调者记录枚举全部转移 ID，供批量逐笔对账。
func (c *Coordinator) AllTransfers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.instances))
	for id := range c.instances {
		ids = append(ids, id)
	}
	return ids
}

func readSide(side string, l *ledger.Ledger, escrowID string) (SideView, AllowedAction) {
	now := l.Now()
	action := AllowedAction{Side: side}
	escrow, err := l.Status(escrowID)
	if err != nil {
		view := SideView{Side: side, Present: false, LocalNow: now.UTC()}
		action.Action = ActionRecoverLock
		action.Note = "账本上不存在该 escrow；若另一侧已锁定，只能先核实账本再决定补发锁定或等待超时"
		return view, action
	}
	view := SideView{
		Side:     side,
		Present:  true,
		Status:   escrow.Status,
		Amount:   escrow.Amount,
		Deadline: escrow.Deadline,
		LocalNow: now.UTC(),
	}
	switch escrow.Status {
	case ledger.StatusLocked:
		view.Locked = true
		if now.Before(escrow.Deadline) {
			remaining := escrow.Deadline.Sub(now)
			view.Remaining = remaining.Round(time.Millisecond).String()
			// 领取窗口：此刻起直到本地截止时间之前；与退款互斥。
			action.Action = ActionClaim
			action.Allowed = true
			action.ValidFrom = now.UTC()
			action.ValidUntil = escrow.Deadline
			action.Note = "窗口内可凭正确 secret 领取；退款窗口尚未开启，领取与退款相撞时账本只接受第一个到达的终态"
		} else {
			view.Overdue = now.Sub(escrow.Deadline).Round(time.Millisecond).String()
			action.Action = ActionRefund
			action.Allowed = true
			action.ValidFrom = escrow.Deadline
			action.Note = "已过本地截止时间，只能退回；迟到的领取会被拒绝，因此领取/退款相撞只形成一个有效终态"
		}
	case ledger.StatusClaimed:
		action.Action = ActionNoneFinal
		action.Note = "已领取，终态；任何退款请求返回 ErrAlreadyFinal"
	case ledger.StatusRefunded:
		action.Action = ActionNoneFinal
		action.Note = "已退回，终态；任何领取请求返回 ErrAlreadyFinal"
	}
	return view, action
}
