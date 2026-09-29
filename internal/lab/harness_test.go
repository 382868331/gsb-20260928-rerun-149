package lab

import (
	"testing"
	"time"

	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/message"
	"crossledgerlab/internal/node"
	"crossledgerlab/internal/protocol"
)

func TestSuccessfulTransferIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 30*time.Second, -90*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	inst, err := rt.Coordinator.Begin("ok-1", "alice", "bob", 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 30*time.Millisecond)
	if got := rt.LedgerA.Balance("alice"); got != 900 {
		t.Fatalf("alice balance = %d, want 900", got)
	}
	inst, err = rt.Coordinator.Reconcile("ok-1")
	if err != nil || inst.Stage != protocol.StageLocked {
		t.Fatalf("reconcile = %s, %v; want locked", inst.Stage, err)
	}

	if err := rt.Coordinator.Claim("ok-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 35*time.Millisecond)
	inst, err = rt.Coordinator.Reconcile("ok-1")
	if err != nil || inst.Stage != protocol.StageClaimed {
		t.Fatalf("final stage = %s, %v", inst.Stage, err)
	}
	if got := rt.LedgerB.Balance("bob"); got != 100 {
		t.Fatalf("bob balance = %d, want 100", got)
	}
	if _, err := rt.LedgerA.Refund(protocol.EscrowID("ok-1", "a")); err != ledger.ErrAlreadyFinal {
		t.Fatalf("refund after claim error = %v, want ErrAlreadyFinal", err)
	}
}

func TestPartialLockDisconnectRestartAndTimeoutRefund(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, time.Second, -2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("partial-1", "alice", "bob", 100, time.Hour); err != nil {
		t.Fatal(err)
	}
	rt.Network.SetDisconnected("ledger-b", true)
	advanceTest(rt, 20*time.Millisecond)

	restarted, err := RestartRuntime(dir, rt.RefClock.Now(), time.Second, -2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Network.SetDisconnected("ledger-b", true)
	inst, err := restarted.Coordinator.Reconcile("partial-1")
	if err != nil || inst.Stage != protocol.StagePartiallyLocked {
		t.Fatalf("restart stage = %s, %v; want partially_locked", inst.Stage, err)
	}
	if got := restarted.LedgerA.Balance("alice"); got != 900 {
		t.Fatalf("alice balance = %d, want 900 locked", got)
	}

	advanceTest(restarted, time.Hour+time.Second)
	restarted.Network.SetDisconnected("ledger-b", false)
	restarted.Network.RestoreDisconnected()
	records, err := restarted.Network.Pump()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 || records[0].Err != ledger.ErrDeadlinePassed {
		t.Fatalf("stale B lock result = %#v, want deadline error", records)
	}
	if err := restarted.Coordinator.RefundExpired("partial-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(restarted, 30*time.Millisecond)
	inst, err = restarted.Coordinator.Reconcile("partial-1")
	if err != nil || inst.Stage != protocol.StagePartiallyRefunded {
		t.Fatalf("refund stage = %s, %v; want partially_refunded", inst.Stage, err)
	}
	if got := restarted.LedgerA.Balance("alice"); got != 1000 {
		t.Fatalf("alice final balance = %d, want 1000", got)
	}
}

func TestDuplicateMessageAppliesOnce(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("32-byte-secret-0123456789abcdef!!")
	payload := node.LockPayload{EscrowID: "manual-a", Transfer: "manual", From: "alice", To: "treasury-a", Amount: 100, HashLock: protocol.HashSecret(secret), Deadline: start.Add(time.Hour)}
	msg, err := rt.Network.Send("test", "ledger-a", message.KindLock, payload, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Network.Pump(); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.NodeA.Handle(msg); err != nil {
		t.Fatalf("duplicate delivery should be ignored, got %v", err)
	}
	if got := rt.LedgerA.Balance("alice"); got != 900 {
		t.Fatalf("alice balance = %d, duplicate lock changed state", got)
	}
}

func advanceTest(rt *Runtime, d time.Duration) {
	rt.RefClock.Advance(d)
	rt.ClockA.Advance(d)
	rt.ClockB.Advance(d)
	if _, err := rt.Network.Pump(); err != nil {
		panic(err)
	}
}
