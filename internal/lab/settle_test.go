package lab

import (
	"errors"
	"testing"
	"time"

	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/protocol"
)

// 600 的转移：第一次领取 170 双侧确认；第二次 220 只在 B 侧落账，A 侧断连。
// 协调者重开并越过截止后，退款必须先处理尚未确定的领取决定，
// 不能把未知的 220 同时作为可退余额。
func TestRefundBlockedUntilPendingClaimSettled(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("fx-1", "alice", "bob", 600, time.Hour); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	// 第一次领取 170：两侧都落账。
	if _, err := rt.Coordinator.DecideClaim("fx-1", 170); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	// A 侧断连后第二次领取 220：只在 B 侧落账，A 侧消息积压、响应丢失。
	rt.Network.SetDisconnected("ledger-a", true)
	if _, err := rt.Coordinator.DecideClaim("fx-1", 220); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)
	if got := rt.LedgerB.Balance("bob"); got != 390 {
		t.Fatalf("bob = %d, want 390（B 侧已落账 170+220）", got)
	}

	// 协调者失联重开，并越过截止。
	rt, err = RestartRuntime(dir, rt.RefClock.Now(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rt.Network.SetDisconnected("ledger-a", true)
	advanceTest(rt, time.Hour+time.Minute)

	// A 侧仍断连：220 金额未知，退款必须被拒绝。
	if err := rt.Coordinator.RefundExpired("fx-1"); !errors.Is(err, protocol.ErrPendingClaims) {
		t.Fatalf("refund err = %v, want ErrPendingClaims（未知金额不能作为可退余额）", err)
	}
	if got := rt.LedgerA.Balance("alice"); got != 400 {
		t.Fatalf("alice = %d, want 400（退款未发生）", got)
	}

	// 恢复连接：原编号重发，A 侧收到迟到的 220 领取，已过截止被明确拒绝。
	rt.Network.SetDisconnected("ledger-a", false)
	rt.Network.RestoreDisconnected()
	if err := rt.Coordinator.SyncClaims("fx-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)
	if err := rt.Coordinator.SyncClaims("fx-1"); err != nil {
		t.Fatal(err)
	}
	inst, err := rt.Coordinator.Get("fx-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(inst.Claims) != 2 {
		t.Fatalf("claims = %+v, want 2 个决定", inst.Claims)
	}
	first, second := inst.Claims[0], inst.Claims[1]
	if first.SideA != protocol.DecisionConfirmed || first.SideB != protocol.DecisionConfirmed {
		t.Fatalf("第一次领取 = %s/%s, want 双侧 confirmed（已确认部分不撤销）", first.SideA, first.SideB)
	}
	if second.SideB != protocol.DecisionConfirmed || second.SideA != protocol.DecisionRejected {
		t.Fatalf("第二次领取 = A:%s B:%s, want A rejected / B confirmed", second.SideA, second.SideB)
	}
	if second.Note == "" {
		t.Fatal("迟到拒绝必须形成可解释的处理结果（Note 为空）")
	}
	if pending, err := rt.Coordinator.PendingClaimAmount("fx-1"); err != nil || pending != 0 {
		t.Fatalf("pending = %d, %v; want 0（不永久卡在未知）", pending, err)
	}

	// 现在退款合法：A 退 430（600-170），B 退 210（600-390），各自守恒。
	if err := rt.Coordinator.RefundExpired("fx-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	escA, err := rt.LedgerA.Status(protocol.EscrowID("fx-1", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if escA.Status != ledger.StatusRefunded || escA.ClaimedAmount != 170 || escA.RefundedAmount != 430 {
		t.Fatalf("A = %s 已领 %d 退 %d, want refunded/170/430", escA.Status, escA.ClaimedAmount, escA.RefundedAmount)
	}
	escB, err := rt.LedgerB.Status(protocol.EscrowID("fx-1", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if escB.Status != ledger.StatusRefunded || escB.ClaimedAmount != 390 || escB.RefundedAmount != 210 {
		t.Fatalf("B = %s 已领 %d 退 %d, want refunded/390/210", escB.Status, escB.ClaimedAmount, escB.RefundedAmount)
	}

	// 整笔结束：双方一致（均为终态）且金额守恒。
	inst, err = rt.Coordinator.Reconcile("fx-1")
	if err != nil || inst.Stage != protocol.StageRefunded {
		t.Fatalf("stage = %s, %v; want refunded", inst.Stage, err)
	}
	if total := rt.LedgerA.TotalBalance() + rt.LedgerA.LockedTotal(); total != 1000 {
		t.Fatalf("A 总额 = %d, want 1000", total)
	}
	if total := rt.LedgerB.TotalBalance() + rt.LedgerB.LockedTotal(); total != 1000 {
		t.Fatalf("B 总额 = %d, want 1000", total)
	}

	// 已确认部分不撤销、不重复领取：再次同步与重发，余额不变。
	if err := rt.Coordinator.SyncClaims("fx-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)
	if got := rt.LedgerB.Balance("bob"); got != 390 {
		t.Fatalf("bob = %d, want 390（重发不得重复入账）", got)
	}
	if got := rt.LedgerA.Balance("alice"); got != 830 {
		t.Fatalf("alice = %d, want 830（400 可用 + 430 退回）", got)
	}

	// 对账报告：无未知金额，两侧守恒，迟到拒绝有据可查。
	reports, err := rt.Coordinator.Audit()
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 {
		t.Fatalf("reports = %+v, want 1 笔", reports)
	}
	report := reports[0]
	if report.Pending != 0 || !report.ConservedA || !report.ConservedB {
		t.Fatalf("report = pending %d conserved %v/%v, want 0/true/true", report.Pending, report.ConservedA, report.ConservedB)
	}
}
