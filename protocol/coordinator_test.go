package protocol

import (
	"context"
	"testing"
	"time"
)

type testHarness struct {
	t            *testing.T
	now          time.Time
	sourceClock  *MutableClock
	destClock    *MutableClock
	networkClock *MutableClock
	source       *Ledger
	dest         *Ledger
	store        ProtocolStore
	network      *Network
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	now := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	sourceClock := NewMutableClock(now)
	destClock := NewMutableClock(now)
	networkClock := NewMutableClock(now)
	root := t.TempDir()
	sourceStore := NewJSONStore(root + "/source")
	destStore := NewJSONStore(root + "/dest")
	sourceLedger := NewLedger("ledger-a", sourceStore, sourceClock)
	destLedger := NewLedger("ledger-b", destStore, destClock)
	protocolStore := NewProtocolStore(root + "/protocol")
	network := NewNetwork(protocolStore, networkClock)
	must(t, sourceLedger.Bootstrap(context.Background(), Account{Name: "alice", Balance: 1000}))
	must(t, sourceLedger.Bootstrap(context.Background(), Account{Name: "settlement", Balance: 0}))
	must(t, destLedger.Bootstrap(context.Background(), Account{Name: "settlement", Balance: 1000}))
	must(t, destLedger.Bootstrap(context.Background(), Account{Name: "bob", Balance: 0}))
	return &testHarness{t: t, now: now, sourceClock: sourceClock, destClock: destClock, networkClock: networkClock, source: sourceLedger, dest: destLedger, store: protocolStore, network: network}
}

func (h *testHarness) coordinator() *Coordinator {
	return NewCoordinator(h.source, h.dest, h.store, h.networkClock)
}

func (h *testHarness) advance(duration time.Duration) {
	h.now = h.now.Add(duration)
	h.sourceClock.Advance(duration)
	h.destClock.Advance(duration)
	h.networkClock.Advance(duration)
}

func (h *testHarness) recreateLedgers() {
	h.source = NewLedger(h.source.name, h.source.store, h.sourceClock)
	h.dest = NewLedger(h.dest.name, h.dest.store, h.destClock)
}

func (h *testHarness) prepare(id string) {
	h.t.Helper()
	_, err := h.coordinator().Create(context.Background(), CreateTransfer{
		ID:             id,
		Sender:         "alice",
		Receiver:       "bob",
		Amount:         100,
		Secret:         "open-sesame",
		DestDeadline:   h.now.Add(10 * time.Minute),
		SourceDeadline: h.now.Add(20 * time.Minute),
	})
	must(h.t, err)
}

func TestClaimAndRefundAreMutuallyExclusive(t *testing.T) {
	h := newHarness(t)
	h.prepare("htlc-1")
	_, _, _, err := h.coordinator().Claim(context.Background(), "htlc-1", "open-sesame", h.now)
	must(t, err)
	h.advance(21 * time.Minute)
	_, _, _, err = h.coordinator().Refund(context.Background(), "htlc-1", h.now)
	if err != ErrConflict {
		t.Fatalf("refund after claim = %v, want ErrConflict", err)
	}
	sourceAccounts, _, err := h.source.Snapshot(context.Background())
	must(t, err)
	destAccounts, _, err := h.dest.Snapshot(context.Background())
	must(t, err)
	if sourceAccounts["alice"].Balance != 900 || sourceAccounts["settlement"].Balance != 100 || destAccounts["settlement"].Balance != 900 || destAccounts["bob"].Balance != 100 {
		t.Fatalf("unexpected balances: source alice=%d settlement=%d destination settlement=%d bob=%d", sourceAccounts["alice"].Balance, sourceAccounts["settlement"].Balance, destAccounts["settlement"].Balance, destAccounts["bob"].Balance)
	}
}

func TestDuplicateClaimSettlesOnce(t *testing.T) {
	h := newHarness(t)
	h.prepare("htlc-duplicate")
	coordinator := h.coordinator()
	first, _, _, err := coordinator.Claim(context.Background(), "htlc-duplicate", "open-sesame", h.now)
	must(t, err)
	second, _, _, err := coordinator.Claim(context.Background(), "htlc-duplicate", "open-sesame", h.now.Add(time.Second))
	must(t, err)
	if first.Status != StatusCommitted || second.Status != StatusCommitted {
		t.Fatalf("claims did not remain idempotent: %+v %+v", first, second)
	}
	_, sourceTransfers, err := h.source.Snapshot(context.Background())
	must(t, err)
	if len(sourceTransfers) != 1 || sourceTransfers["htlc-duplicate"].Status != TransferSettled {
		t.Fatalf("transfer not settled exactly once: %+v", sourceTransfers)
	}
}

func TestTimeoutRefundOnlyAfterBothLedgerDeadlines(t *testing.T) {
	h := newHarness(t)
	h.prepare("htlc-timeout")
	h.advance(11 * time.Minute)
	_, _, _, err := h.coordinator().Refund(context.Background(), "htlc-timeout", h.now)
	if err != ErrTimeout {
		t.Fatalf("refund after destination deadline only = %v, want ErrTimeout", err)
	}
	observed, err := h.coordinator().Observe(context.Background(), "htlc-timeout")
	must(t, err)
	if observed.Source.Status != TransferHeld || observed.Dest.Status != TransferHeld {
		t.Fatalf("funds were not held: source=%s destination=%s", observed.Source.Status, observed.Dest.Status)
	}
	h.advance(10 * time.Minute)
	instance, source, dest, err := h.coordinator().Refund(context.Background(), "htlc-timeout", h.now)
	must(t, err)
	if instance.Status != StatusRefunded || source.Status != TransferRefunded || dest.Status != TransferRefunded {
		t.Fatalf("unexpected refund state: %s %s %s", instance.Status, source.Status, dest.Status)
	}
}

func TestRecoverFromActualLedgerAfterDestinationClaimAndRestart(t *testing.T) {
	h := newHarness(t)
	h.prepare("htlc-partial")
	_, err := h.dest.Claim(context.Background(), "htlc-partial", "open-sesame")
	must(t, err)
	instances, err := h.store.LoadInstances(context.Background())
	must(t, err)
	instance := instances["htlc-partial"]
	instance.Status = StatusAborted
	instances["htlc-partial"] = instance
	must(t, h.store.SaveInstances(context.Background(), instances))
	h.recreateLedgers()
	instance, source, dest, err := h.coordinator().RecoverTransfer(context.Background(), "htlc-partial", h.now)
	must(t, err)
	if instance.Status != StatusCommitted || source.Status != TransferSettled || dest.Status != TransferSettled {
		t.Fatalf("recovery did not finish claim: %s %s %s", instance.Status, source.Status, dest.Status)
	}
}

func TestPreparedSideDisconnectShowsLockedFundsAndRefundTime(t *testing.T) {
	h := newHarness(t)
	h.prepare("htlc-disconnect")
	h.network.SetDisconnected(Source, true)
	message := Message{ID: "claim-1", TransferID: "htlc-disconnect", From: Destination, To: Source, Kind: "claim", Secret: "open-sesame", CreatedAt: h.now}
	duplicate := message
	sent, err := h.network.Send(context.Background(), message)
	must(t, err)
	if sent {
		t.Fatal("message delivered while source was disconnected")
	}
	_, err = h.network.Send(context.Background(), duplicate)
	if err != nil {
		t.Fatalf("duplicate send should be harmless: %v", err)
	}
	messages, err := h.network.Messages(context.Background())
	must(t, err)
	if len(messages) != 1 {
		t.Fatalf("duplicate message was recorded %d times", len(messages))
	}
	h.advance(15 * time.Minute)
	observed, err := h.coordinator().Observe(context.Background(), "htlc-disconnect")
	must(t, err)
	if observed.Source.Status != TransferHeld || observed.Dest.Status != TransferHeld {
		t.Fatalf("funds should remain locked on both ledgers: %s %s", observed.Source.Status, observed.Dest.Status)
	}
	if !h.now.Before(observed.Source.LockUntil) || !h.now.After(observed.Dest.LockUntil) {
		t.Fatal("destination should be refundable while source remains locked")
	}
	_, err = h.dest.Refund(context.Background(), "htlc-disconnect")
	must(t, err)
	_, _, _, err = h.coordinator().Refund(context.Background(), "htlc-disconnect", h.now)
	if err != ErrTimeout {
		t.Fatalf("source refund before its deadline = %v, want ErrTimeout", err)
	}
	h.advance(6 * time.Minute)
	instance, source, dest, err := h.coordinator().RecoverTransfer(context.Background(), "htlc-disconnect", h.now)
	must(t, err)
	if instance.Status != StatusRefunded || source.Status != TransferRefunded || dest.Status != TransferRefunded {
		t.Fatalf("funds were not refunded after source deadline: %s %s %s", instance.Status, source.Status, dest.Status)
	}
}

func TestVirtualConfirmationDelay(t *testing.T) {
	h := newHarness(t)
	h.network.SetDelay(Source, 2*time.Minute)
	message := Message{ID: "delayed-claim", TransferID: "x", From: Destination, To: Source, Kind: "claim"}
	delivered, err := h.network.Send(context.Background(), message)
	must(t, err)
	if delivered {
		t.Fatal("message should be delayed")
	}
	h.advance(time.Minute)
	count, err := h.network.DeliverDue(context.Background())
	must(t, err)
	if count != 0 {
		t.Fatalf("message delivered early: %d", count)
	}
	h.advance(time.Minute)
	count, err = h.network.DeliverDue(context.Background())
	must(t, err)
	if count != 1 {
		t.Fatalf("message not delivered after delay: %d", count)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
