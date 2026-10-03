package lab

import (
	"fmt"
	"testing"
	"time"

	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/protocol"
)

func TestConcurrentTransfersReserveFunds(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// alice 共 1000；同时发起三笔各 400，任意两笔成功后第三笔必因资金被占用而失败。
	if _, err := rt.Coordinator.Begin("multi-1", "alice", "bob", 400, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("multi-2", "alice", "bob", 400, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("multi-3", "alice", "bob", 400, time.Hour); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)
	if got := rt.LedgerA.Balance("alice"); got != 200 {
		t.Fatalf("alice balance = %d, want 200（800 被两笔锁定占用）", got)
	}
	missing := 0
	for _, id := range []string{"multi-1", "multi-2", "multi-3"} {
		if _, err := rt.LedgerA.Status(protocol.EscrowID(id, "a")); err == ledger.ErrMissing {
			missing++
		}
	}
	if missing != 1 {
		t.Fatalf("A 侧应有且仅有一笔未形成锁，实际 %d 笔", missing)
	}
	// 已锁定的资金不能再次用于另一笔：直接对账本重复占用应被拒绝。
	secret := []byte("another-32-byte-secret-0123456789ab")
	_, err = rt.LedgerA.Lock(ledger.LockRequest{ID: "extra-a", Transfer: "extra", From: "alice", To: "treasury-a", Amount: 300, HashLock: protocol.HashSecret(secret), Deadline: start.Add(time.Hour)})
	if err != ledger.ErrInsufficientFunds {
		t.Fatalf("重复占用已锁资金 err = %v, want ErrInsufficientFunds", err)
	}
	// 对账报告逐笔展示仍锁金额与动作。
	reports, err := rt.Coordinator.Audit()
	if err != nil {
		t.Fatal(err)
	}
	var totalLocked uint64
	for _, report := range reports {
		totalLocked += report.A.Locked
	}
	if totalLocked != 800 {
		t.Fatalf("audit 总锁定 = %d, want 800", totalLocked)
	}
}

func TestClaimRefundCollisionSingleTerminalState(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("collision-secret-32-byte-0123456789")
	deadline := rt.ClockA.Now()
	if _, err := rt.LedgerA.Lock(ledger.LockRequest{ID: "race-a", Transfer: "race", From: "alice", To: "treasury-a", Amount: 100, HashLock: protocol.HashSecret(secret), Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	// 在截止点同时发起领取与退款，只能形成一个有效终态。
	const goroutines = 16
	errs := make(chan error, goroutines*2)
	for i := 0; i < goroutines; i++ {
		claimID := fmt.Sprintf("race-claim-%d", i)
		go func() {
			_, err := rt.LedgerA.Claim("race-a", secret, claimID, 100)
			errs <- err
		}()
		go func() {
			_, err := rt.LedgerA.Refund("race-a")
			errs <- err
		}()
	}
	for i := 0; i < goroutines*2; i++ {
		<-errs
	}
	esc, err := rt.LedgerA.Status("race-a")
	if err != nil {
		t.Fatal(err)
	}
	if esc.Status != ledger.StatusClaimed && esc.Status != ledger.StatusRefunded {
		t.Fatalf("终态 = %s, want claimed 或 refunded", esc.Status)
	}
	total := rt.LedgerA.Balance("alice") + rt.LedgerA.Balance("treasury-a")
	if total != 1000 {
		t.Fatalf("账本总额 = %d, want 1000（撞单不得双重支付）", total)
	}
}

func TestLongOutageAuditFromLedgers(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 30*time.Second, -90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("long-1", "alice", "bob", 100, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("long-2", "alice", "bob", 250, 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	rt.Network.SetDisconnected("ledger-b", true)
	advanceTest(rt, 20*time.Millisecond)

	// 协调者长时间失联：推进超过 long-1 截止、未到 long-2 截止，然后重启。
	restarted, err := RestartRuntime(dir, rt.RefClock.Now(), 30*time.Second, -90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	restarted.RefClock.Advance(90 * time.Minute)
	restarted.ClockA.Advance(90 * time.Minute)
	restarted.ClockB.Advance(90 * time.Minute)
	restarted.Network.SetDisconnected("ledger-b", true)

	reports, err := restarted.Coordinator.Audit()
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]protocol.TransferReport{}
	for _, report := range reports {
		byID[report.ID] = report
	}
	r1 := byID["long-1"]
	if r1.Stage != protocol.StagePartiallyLocked || r1.A.Locked != 100 {
		t.Fatalf("long-1 report = %+v, want partially_locked 且 A 仍锁 100", r1)
	}
	if len(r1.A.Actions) == 0 || r1.A.Actions[0][:6] != "refund" {
		t.Fatalf("long-1 A 动作 = %v, want refund（已过截止）", r1.A.Actions)
	}
	r2 := byID["long-2"]
	if r2.A.Locked != 250 {
		t.Fatalf("long-2 A 锁定 = %d, want 250", r2.A.Locked)
	}
	if len(r2.A.Actions) == 0 || r2.A.Actions[0][:5] != "claim" {
		t.Fatalf("long-2 A 动作 = %v, want claim（未到截止）", r2.A.Actions)
	}
	if got := restarted.LedgerA.Balance("alice"); got != 650 {
		t.Fatalf("alice = %d, want 650（350 仍被两笔占用）", got)
	}
}
