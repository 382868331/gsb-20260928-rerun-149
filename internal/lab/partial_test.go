package lab

import (
	"testing"
	"time"

	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/protocol"
)

func lockEscrow(t *testing.T, rt *Runtime, id string, amount uint64, deadline time.Time) []byte {
	t.Helper()
	secret := []byte("partial-secret-32-bytes-0123456789!")
	if _, err := rt.LedgerB.Lock(ledger.LockRequest{ID: id, Transfer: id, From: "treasury-b", To: "bob", Amount: amount, HashLock: protocol.HashSecret(secret), Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	return secret
}

func TestPartialClaimsTraceableAndConserved(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	secret := lockEscrow(t, rt, "part-1-b", 100, start.Add(time.Hour))

	// 第一次分次领取 40：进入 partially_claimed，而不是整笔结清。
	esc, record, err := rt.LedgerB.ClaimPartial("part-1-b", secret, "c1", 40)
	if err != nil {
		t.Fatal(err)
	}
	if esc.Status != ledger.StatusPartiallyClaimed {
		t.Fatalf("status = %s, want partially_claimed", esc.Status)
	}
	if record.Amount != 40 || record.Confirmed != 40 || record.Remaining != 60 {
		t.Fatalf("record = %+v, want 本次40/已确认40/剩余60", record)
	}

	// 重复请求与丢失后重发：相同 claimID 直接返回首次记录，不重复加款。
	again, againRecord, err := rt.LedgerB.ClaimPartial("part-1-b", secret, "c1", 40)
	if err != nil {
		t.Fatal(err)
	}
	if againRecord.Confirmed != 40 || again.ClaimedAmount != 40 {
		t.Fatalf("duplicate claim = %+v/%+v, want 仍确认 40", again, againRecord)
	}
	if got := rt.LedgerB.Balance("bob"); got != 40 {
		t.Fatalf("bob = %d, want 40（重复请求不得多扣余额）", got)
	}

	// 第二次分次领取 30。
	esc, record, err = rt.LedgerB.ClaimPartial("part-1-b", secret, "c2", 30)
	if err != nil {
		t.Fatal(err)
	}
	if record.Confirmed != 70 || record.Remaining != 30 || esc.Status != ledger.StatusPartiallyClaimed {
		t.Fatalf("second record = %+v status=%s, want 确认70/剩余30", record, esc.Status)
	}

	// 超额领取被拒绝。
	if _, _, err := rt.LedgerB.ClaimPartial("part-1-b", secret, "c3", 31); err != ledger.ErrInvalidAmount {
		t.Fatalf("over-claim err = %v, want ErrInvalidAmount", err)
	}

	// 到期后只退回未领取的 30，已确认 70 不退。
	rt.ClockB.Advance(time.Hour + time.Second)
	esc, err = rt.LedgerB.Refund("part-1-b")
	if err != nil {
		t.Fatal(err)
	}
	if esc.RefundedAmount != 30 || esc.ClaimedAmount != 70 {
		t.Fatalf("refund = %+v, want 退30已领70", esc)
	}
	if got := rt.LedgerB.Balance("treasury-b"); got != 930 {
		t.Fatalf("treasury-b = %d, want 930", got)
	}
	if got := rt.LedgerB.Balance("bob"); got != 70 {
		t.Fatalf("bob = %d, want 70", got)
	}
	if total := rt.LedgerB.TotalBalance() + rt.LedgerB.LockedTotal(); total != 1000 {
		t.Fatalf("B 总额 = %d, want 1000（守恒）", total)
	}
}

func TestClaimRefundDecisionBoundary(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	deadline := start.Add(time.Hour)
	secret := lockEscrow(t, rt, "bound-1-b", 100, deadline)

	// 截止前最后一刻：领取有效，退款无效。
	rt.ClockB.Advance(time.Hour - time.Second)
	if _, err := rt.LedgerB.Refund("bound-1-b"); err != ledger.ErrDeadlineNotPassed {
		t.Fatalf("early refund err = %v, want ErrDeadlineNotPassed", err)
	}
	if _, _, err := rt.LedgerB.ClaimPartial("bound-1-b", secret, "c1", 60); err != nil {
		t.Fatalf("pre-deadline claim err = %v", err)
	}

	// 截止点：领取无效，退款有效且只退剩余 40；未知领取不能悬而未决又被退款。
	rt.ClockB.Advance(time.Second)
	if _, _, err := rt.LedgerB.ClaimPartial("bound-1-b", secret, "c2", 40); err != ledger.ErrDeadlinePassed {
		t.Fatalf("at-deadline claim err = %v, want ErrDeadlinePassed", err)
	}
	esc, err := rt.LedgerB.Refund("bound-1-b")
	if err != nil {
		t.Fatal(err)
	}
	if esc.RefundedAmount != 40 {
		t.Fatalf("refunded = %d, want 40（已确认 60 不退）", esc.RefundedAmount)
	}
	if total := rt.LedgerB.TotalBalance() + rt.LedgerB.LockedTotal(); total != 1000 {
		t.Fatalf("B 总额 = %d, want 1000", total)
	}
}

func TestCoordinatorRestartContinuesSameClaimDecision(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("re-1", "alice", "bob", 100, time.Hour); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	// 决定第一次分次领取 40，但在投递前协调者失联。
	decision, err := rt.Coordinator.DecideClaim("re-1", 40)
	if err != nil {
		t.Fatal(err)
	}

	// 重启后继续追查原决定：相同 claimID 与消息 ID，不以新编号重做。
	restarted, err := RestartRuntime(dir, rt.RefClock.Now(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Coordinator.SyncClaims("re-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(restarted, 40*time.Millisecond)
	// 回应丢失再重发一次，仍只生效一次。
	if err := restarted.Coordinator.SyncClaims("re-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(restarted, 40*time.Millisecond)

	if got := restarted.LedgerB.Balance("bob"); got != 40 {
		t.Fatalf("bob = %d, want 40（重发不得重复入账）", got)
	}
	inst, err := restarted.Coordinator.Get("re-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(inst.Claims) != 1 || inst.Claims[0].ClaimID != decision.ClaimID || !inst.Claims[0].Confirmed {
		t.Fatalf("claims = %+v, want 原编号 %s 已确认", inst.Claims, decision.ClaimID)
	}
	esc, err := restarted.LedgerB.Status(protocol.EscrowID("re-1", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if esc.Status != ledger.StatusPartiallyClaimed || esc.ClaimedAmount != 40 {
		t.Fatalf("escrow = %s/%d, want partially_claimed/40", esc.Status, esc.ClaimedAmount)
	}
	if total := restarted.LedgerB.TotalBalance() + restarted.LedgerB.LockedTotal(); total != 1000 {
		t.Fatalf("B 总额 = %d, want 1000", total)
	}
}
