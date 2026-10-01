package lab

import (
	"testing"
	"time"

	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/message"
	"crossledgerlab/internal/node"
	"crossledgerlab/internal/protocol"
)

func testStart() time.Time {
	return time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
}

// 同一参与者同时发起多笔转移：先锁定的金额不能再次用于另一笔。
func TestConcurrentTransfersDoNotDoubleSpendLockedFunds(t *testing.T) {
	dir := t.TempDir()
	rt, err := NewRuntime(dir, testStart(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 两笔各 600：alice 的 1000 只够锁一笔；B 侧金库注资充足，可独立锁两笔。
	if err := rt.LedgerB.Credit("treasury-b", 1000); err != nil {
		t.Fatal(err)
	}

	if _, err := rt.Coordinator.Begin("multi-1", "alice", "bob", 600, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("multi-2", "alice", "bob", 600, 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 35*time.Millisecond)

	if got := rt.LedgerA.LockedBalance("alice"); got != 600 {
		t.Fatalf("alice locked = %d, want 600", got)
	}
	if got := rt.LedgerA.AvailableBalance("alice"); got != 400 {
		t.Fatalf("alice available = %d, want 400", got)
	}
	a1, err := rt.LedgerA.Status(protocol.EscrowID("multi-1", "a"))
	if err != nil || a1.Status != ledger.StatusLocked {
		t.Fatalf("multi-1 A escrow = %v, %v; want locked", a1, err)
	}
	if _, err := rt.LedgerA.Status(protocol.EscrowID("multi-2", "a")); err != ledger.ErrMissing {
		t.Fatalf("multi-2 A escrow err = %v, want ErrMissing (资金不足, 不形成锁)", err)
	}
	b2, err := rt.LedgerB.Status(protocol.EscrowID("multi-2", "b"))
	if err != nil || b2.Status != ledger.StatusLocked {
		t.Fatalf("multi-2 B escrow = %v, %v; want locked (部分锁定)", b2, err)
	}
	if got := rt.LedgerB.LockedBalance("treasury-b"); got != 1200 {
		t.Fatalf("treasury-b locked = %d, want 1200", got)
	}

	report2, _, err := rt.Coordinator.ReconcileReport("multi-2")
	if err != nil {
		t.Fatal(err)
	}
	if report2.Stage != protocol.StagePartiallyLocked {
		t.Fatalf("multi-2 stage = %s, want partially_locked", report2.Stage)
	}
	if report2.LockedA != 0 || report2.LockedB != 600 {
		t.Fatalf("multi-2 locked amounts = A:%d B:%d, want A:0 B:600", report2.LockedA, report2.LockedB)
	}
	var bAction *protocol.AllowedAction
	for i := range report2.Actions {
		if report2.Actions[i].Side == "b" {
			bAction = &report2.Actions[i]
		}
	}
	if bAction == nil || bAction.Action != protocol.ActionClaim || !bAction.Allowed {
		t.Fatalf("multi-2 B action = %+v, want allowed claim window", bAction)
	}
}

// 领取与退款在截止点相撞：截止前领取成功后，迟到退款失败；反向亦然。
func TestClaimRefundCollisionHasSingleFinalState(t *testing.T) {
	dir := t.TempDir()
	rt, err := NewRuntime(dir, testStart(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := rt.Coordinator.Begin("race-1", "alice", "bob", 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 35*time.Millisecond)

	claimed, err := rt.LedgerB.Claim(protocol.EscrowID("race-1", "b"), inst.Secret)
	if err != nil {
		t.Fatalf("claim inside window: %v", err)
	}
	advanceTest(rt, time.Hour+time.Millisecond)
	if _, err := rt.LedgerB.Refund(claimed.ID); err != ledger.ErrAlreadyFinal {
		t.Fatalf("refund after claim = %v, want ErrAlreadyFinal", err)
	}
	if got := rt.LedgerB.Balance("bob"); got != 100 {
		t.Fatalf("bob balance = %d, want 100 (仅一次入账)", got)
	}

	// 反向相撞：另一笔先超时退款成功，迟到的领取必须失败，不发款。
	rt2, err := NewRuntime(t.TempDir(), testStart(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	inst2, err := rt2.Coordinator.Begin("race-2", "alice", "bob", 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceTest(rt2, 35*time.Millisecond)
	advanceTest(rt2, time.Hour+time.Millisecond)
	if _, err := rt2.LedgerB.Refund(protocol.EscrowID("race-2", "b")); err != nil {
		t.Fatalf("refund after deadline: %v", err)
	}
	if _, err := rt2.LedgerB.Claim(protocol.EscrowID("race-2", "b"), inst2.Secret); err != ledger.ErrAlreadyFinal {
		t.Fatalf("late claim = %v, want ErrAlreadyFinal", err)
	}
	if got := rt2.LedgerB.Balance("bob"); got != 0 {
		t.Fatalf("bob balance = %d, want 0 (退款后不发款)", got)
	}
}

// 重复的锁定/领取/退款消息在账本层面幂等，不会重复扣款或加款。
func TestRepeatedOperationsAreIdempotentAtLedger(t *testing.T) {
	dir := t.TempDir()
	rt, err := NewRuntime(dir, testStart(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := rt.Coordinator.Begin("dup-1", "alice", "bob", 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 35*time.Millisecond)

	// 同一 escrow 用相同条款再次锁定：幂等返回，不二次扣款。
	repeatLock := node.LockPayload{
		EscrowID: protocol.EscrowID("dup-1", "a"),
		Transfer: "dup-1", From: "alice", To: protocol.TreasuryA,
		Amount: 100, HashLock: inst.HashLock, Deadline: inst.Deadline,
	}
	msg, err := rt.Network.Send("test", "ledger-a", message.KindLock, repeatLock, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Network.Pump(); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.NodeA.Handle(msg); err != nil {
		t.Fatalf("重复锁定消息应被忽略: %v", err)
	}
	if got := rt.LedgerA.Balance("alice"); got != 900 {
		t.Fatalf("alice balance = %d, want 900", got)
	}

	// 新消息 ID 包装同一领取意图，重复领取只入账一次。
	for i := 0; i < 2; i++ {
		p := node.SecretPayload{EscrowID: protocol.EscrowID("dup-1", "b"), Secret: inst.Secret}
		if _, err := rt.Network.Send("test", "ledger-b", message.KindClaim, p, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := rt.Network.Pump(); err != nil {
			t.Fatal(err)
		}
	}
	if got := rt.LedgerB.Balance("bob"); got != 100 {
		t.Fatalf("bob after duplicate claims = %d, want 100", got)
	}

	// 重复退款在已领取终态上一律拒绝；未锁定侧无副作用。
	advanceTest(rt, time.Hour+time.Millisecond)
	for i := 0; i < 2; i++ {
		p := node.IDPayload{EscrowID: protocol.EscrowID("dup-1", "b")}
		if _, err := rt.Network.Send("test", "ledger-b", message.KindRefund, p, 0); err != nil {
			t.Fatal(err)
		}
		records, err := rt.Network.Pump()
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || records[0].Err != ledger.ErrAlreadyFinal {
			t.Fatalf("对已领取 escrow 的重复退款 records=%#v, 必须返回 ErrAlreadyFinal", records)
		}
	}
	if got := rt.LedgerB.Balance("bob"); got != 100 {
		t.Fatalf("bob after rejected refunds = %d, want 100", got)
	}
}

// 协调者长时间失联后重启：逐笔重新读取两侧账本出报告，而不是依据过期日志发款。
func TestCoordinatorRestartReconcilesEveryTransferFromLedgers(t *testing.T) {
	dir := t.TempDir()
	offsetA := 30 * time.Second
	offsetB := -90 * time.Second
	rt, err := NewRuntime(dir, testStart(), offsetA, offsetB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("batch-1", "alice", "bob", 100, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("batch-2", "alice", "bob", 200, 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	rt.Network.SetDisconnected("ledger-b", true)
	advanceTest(rt, 20*time.Millisecond)

	// 协调者失联很久后才恢复，用全新进程重新打开账本。
	advanceTest(rt, 3*time.Hour)
	restarted, err := RestartRuntime(dir, rt.RefClock.Now(), offsetA, offsetB)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Network.SetDisconnected("ledger-b", false)
	restarted.Network.RestoreDisconnected()
	if _, err := restarted.Network.Pump(); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"batch-1", "batch-2"} {
		report, locks, err := restarted.Coordinator.ReconcileReport(id)
		if err != nil {
			t.Fatal(err)
		}
		if report.Stage != protocol.StagePartiallyLocked {
			t.Fatalf("%s stage = %s, want partially_locked", id, report.Stage)
		}
		if report.LockedA != report.Amount || report.LockedB != 0 {
			t.Fatalf("%s locked = A:%d B:%d, want A:%d B:0", id, report.LockedA, report.LockedB, report.Amount)
		}
		for _, action := range report.Actions {
			if action.Side != "a" {
				continue
			}
			if action.Action != protocol.ActionRefund || !action.Allowed {
				t.Fatalf("%s A action = %+v, want allowed refund (A 本地时钟已过截止)", id, action)
			}
		}
		if len(locks) != 2 || locks[0].Ledger != "A" {
			t.Fatalf("lock summary = %+v", locks)
		}
	}
	if got := restarted.LedgerA.LockedBalance("alice"); got != 300 {
		t.Fatalf("alice total locked = %d, want 300 (100+200)", got)
	}

	if err := restarted.Coordinator.RefundExpired("batch-1"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Coordinator.RefundExpired("batch-2"); err != nil {
		t.Fatal(err)
	}
	advanceTest(restarted, 30*time.Millisecond)
	if got := restarted.LedgerA.Balance("alice"); got != 1000 {
		t.Fatalf("alice final balance = %d, want 1000", got)
	}
	if got := restarted.LedgerA.LockedBalance("alice"); got != 0 {
		t.Fatalf("alice still locked = %d, want 0", got)
	}

	// 重复退款消息（新消息 ID）在终态上幂等，不重复加钱。
	p := node.IDPayload{EscrowID: protocol.EscrowID("batch-1", "a")}
	if _, err := restarted.Network.Send("test", "ledger-a", message.KindRefund, p, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Network.Pump(); err != nil {
		t.Fatal(err)
	}
	if got := restarted.LedgerA.Balance("alice"); got != 1000 {
		t.Fatalf("alice after duplicate refund = %d, want 1000", got)
	}
}
