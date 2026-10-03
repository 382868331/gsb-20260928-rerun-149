package lab

import (
	"testing"
	"time"

	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/protocol"
)

func TestPartialClaimsAccumulateAndRefundRemainder(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("part-1", "alice", "bob", 500, time.Hour); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	claim1, err := rt.Coordinator.ClaimPartial("part-1", 200)
	if err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)
	inst, err := rt.Coordinator.Reconcile("part-1")
	if err != nil || inst.Stage != protocol.StagePartiallyClaimed {
		t.Fatalf("stage = %s, %v; want partially_claimed", inst.Stage, err)
	}
	if got := rt.LedgerB.Balance("bob"); got != 200 {
		t.Fatalf("bob = %d, want 200", got)
	}
	esc, err := rt.LedgerB.Status(protocol.EscrowID("part-1", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if len(esc.Claims) != 1 || esc.Claims[0].Amount != 200 || esc.Claims[0].ConfirmedTotal != 200 || esc.Claims[0].Remaining != 300 {
		t.Fatalf("claim record = %+v, want 200/200/300", esc.Claims)
	}

	if _, err := rt.LedgerB.Claim(protocol.EscrowID("part-1", "b"), inst.Secret, claim1, 200); err != nil {
		t.Fatal(err)
	}
	if got := rt.LedgerB.Balance("bob"); got != 200 {
		t.Fatalf("dup confirm bob = %d, want 200", got)
	}
	if _, err := rt.LedgerB.Claim(protocol.EscrowID("part-1", "b"), inst.Secret, claim1, 500); err != nil {
		t.Fatal(err)
	}
	if got := rt.LedgerB.Balance("bob"); got != 200 {
		t.Fatalf("replay bob = %d, want 200", got)
	}

	if _, err := rt.Coordinator.ClaimPartial("part-1", 150); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)
	esc, err = rt.LedgerB.Status(protocol.EscrowID("part-1", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if len(esc.Claims) != 2 || esc.Claims[1].ConfirmedTotal != 350 || esc.Claims[1].Remaining != 150 {
		t.Fatalf("claims = %+v, want total 350 remaining 150", esc.Claims)
	}

	if _, err := rt.LedgerB.Claim(protocol.EscrowID("part-1", "b"), inst.Secret, "part-1-claim-x", 151); err != ledger.ErrExceedsRemaining {
		t.Fatalf("over-claim err = %v, want ErrExceedsRemaining", err)
	}

	advanceTest(rt, time.Hour)
	if err := rt.Coordinator.RefundExpired("part-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)
	inst, err = rt.Coordinator.Reconcile("part-1")
	if err != nil || inst.Stage != protocol.StageMixedSettled {
		t.Fatalf("stage = %s, %v; want mixed_settled", inst.Stage, err)
	}
	if got := rt.LedgerA.Balance("alice") + rt.LedgerA.Balance("treasury-a"); got != 1000 {
		t.Fatalf("A total = %d, want 1000", got)
	}
	if got := rt.LedgerB.Balance("treasury-b") + rt.LedgerB.Balance("bob"); got != 1000 {
		t.Fatalf("B total = %d, want 1000", got)
	}
	if got := rt.LedgerA.Balance("alice"); got != 650 {
		t.Fatalf("alice = %d, want 650", got)
	}
	if _, err := rt.LedgerA.Refund(protocol.EscrowID("part-1", "a")); err != nil {
		t.Fatal(err)
	}
	if got := rt.LedgerA.Balance("alice"); got != 650 {
		t.Fatalf("dup refund alice = %d, want 650", got)
	}
}

func TestRefundBlockedWhileClaimPendingAndResumedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("pend-1", "alice", "bob", 300, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	rt.Network.SetDisconnected("ledger-b", true)
	claimID, err := rt.Coordinator.ClaimPartial("pend-1", 120)
	if err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	restarted, err := RestartRuntime(dir, rt.RefClock.Now(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Network.SetDisconnected("ledger-b", true)
	advanceTest(restarted, time.Hour)

	if err := restarted.Coordinator.RefundExpired("pend-1"); err != protocol.ErrPendingClaims {
		t.Fatalf("refund err = %v, want ErrPendingClaims", err)
	}
	pending, err := restarted.Coordinator.PendingClaims("pend-1")
	if err != nil || len(pending) != 1 || pending[0] != claimID {
		t.Fatalf("pending = %v, %v; want [%s]", pending, err, claimID)
	}

	restarted.Network.SetDisconnected("ledger-b", false)
	if _, err := restarted.Coordinator.ResumePending("pend-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(restarted, 40*time.Millisecond)
	if got := restarted.LedgerA.Balance("treasury-a"); got != 120 {
		t.Fatalf("treasury-a = %d, want 120 (no double apply after resend)", got)
	}
	esc, err := restarted.LedgerA.Status(protocol.EscrowID("pend-1", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(esc.Claims) != 1 || esc.Claims[0].ID != claimID {
		t.Fatalf("A claims = %+v, want single record with original ID", esc.Claims)
	}
}

func TestDeadlineBoundaryClaimAndRefundExclusive(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("boundary-secret-32-byte-012345678")
	deadline := rt.ClockA.Now()
	if _, err := rt.LedgerA.Lock(ledger.LockRequest{ID: "bound-a", Transfer: "bound", From: "alice", To: "treasury-a", Amount: 100, HashLock: protocol.HashSecret(secret), Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.LedgerA.Refund("bound-a"); err != ledger.ErrDeadlineNotPassed {
		t.Fatalf("refund at deadline err = %v, want ErrDeadlineNotPassed", err)
	}
	if _, err := rt.LedgerA.Claim("bound-a", secret, "bound-claim-1", 40); err != nil {
		t.Fatalf("claim at deadline err = %v, want nil", err)
	}
	rt.ClockA.Advance(time.Nanosecond)
	if _, err := rt.LedgerA.Claim("bound-a", secret, "bound-claim-2", 10); err != ledger.ErrDeadlinePassed {
		t.Fatalf("claim after deadline err = %v, want ErrDeadlinePassed", err)
	}
	esc, err := rt.LedgerA.Refund("bound-a")
	if err != nil {
		t.Fatal(err)
	}
	if esc.Refunded != 60 || esc.Claimed != 40 || esc.Status != ledger.StatusMixedSettled {
		t.Fatalf("escrow = %+v, want claimed 40 refunded 60 mixed_settled", esc)
	}
	if got := rt.LedgerA.Balance("alice") + rt.LedgerA.Balance("treasury-a"); got != 1000 {
		t.Fatalf("total = %d, want 1000", got)
	}
}
