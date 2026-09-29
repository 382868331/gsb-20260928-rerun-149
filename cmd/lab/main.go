package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"xtransfer-lab/protocol"
)

func main() {
	ctx := context.Background()
	root := "runtime-data"
	fatal(os.RemoveAll(root))

	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	sourceClock := protocol.NewMutableClock(start)
	destClock := protocol.NewMutableClock(start.Add(30 * time.Second))
	networkClock := protocol.NewMutableClock(start)
	source := protocol.NewLedger("ledger-a", protocol.NewJSONStore(filepath.Join(root, "ledger-a")), sourceClock)
	dest := protocol.NewLedger("ledger-b", protocol.NewJSONStore(filepath.Join(root, "ledger-b")), destClock)
	store := protocol.NewProtocolStore(filepath.Join(root, "protocol"))
	network := protocol.NewNetwork(store, networkClock)

	fatal(source.Bootstrap(ctx, protocol.Account{Name: "alice", Balance: 1000}, protocol.Account{Name: "settlement", Balance: 0}))
	fatal(dest.Bootstrap(ctx, protocol.Account{Name: "settlement", Balance: 1000}, protocol.Account{Name: "bob", Balance: 0}))

	coordinator := protocol.NewCoordinator(source, dest, store, networkClock)
	id := "lab-htlc-1"
	_, err := coordinator.Create(ctx, protocol.CreateTransfer{
		ID:             id,
		Sender:         "alice",
		Receiver:       "bob",
		Amount:         100,
		Secret:         "test-secret",
		DestDeadline:   start.Add(10 * time.Minute),
		SourceDeadline: start.Add(20 * time.Minute),
	})
	fatal(err)
	line("T+0s", "两侧均已锁定；ledger-b 10:10 后可退，ledger-a 10:20 后可退", start, sourceClock, destClock, coordinator, source, dest, id)

	network.SetDisconnected(protocol.Source, true)
	network.SetDelay(protocol.Source, 30*time.Second)
	_, err = network.Send(ctx, protocol.Message{ID: "claim-1", TransferID: id, From: protocol.Destination, To: protocol.Source, Kind: "claim", Secret: "test-secret"})
	fatal(err)
	_, err = dest.Claim(ctx, id, "test-secret")
	fatal(err)
	line("T+30s", "ledger-b 已领取；ledger-a 断开，确认消息滞留，alice 的 100 仍锁在 ledger-a", start.Add(30*time.Second), sourceClock, destClock, coordinator, source, dest, id)

	sourceClock.Advance(30 * time.Second)
	destClock.Advance(30 * time.Second)
	networkClock.Advance(30 * time.Second)
	network.SetDisconnected(protocol.Source, false)
	delivered, err := network.DeliverDue(ctx)
	fatal(err)
	source = protocol.NewLedger("ledger-a", source.Store(), sourceClock)
	coordinator = protocol.NewCoordinator(source, dest, store, networkClock)
	_, _, _, err = coordinator.RecoverTransfer(ctx, id, networkClock.Now())
	fatal(err)
	line(fmt.Sprintf("T+60s, delivered=%d", delivered), "协调者重启后读取两侧实际 HTLC；用 ledger-b 已公开的密钥补领 ledger-a", start.Add(time.Minute), sourceClock, destClock, coordinator, source, dest, id)

	timeoutRoot := filepath.Join(root, "timeout")
	tSourceClock := protocol.NewMutableClock(start)
	tDestClock := protocol.NewMutableClock(start)
	tSource := protocol.NewLedger("ledger-a", protocol.NewJSONStore(filepath.Join(timeoutRoot, "ledger-a")), tSourceClock)
	tDest := protocol.NewLedger("ledger-b", protocol.NewJSONStore(filepath.Join(timeoutRoot, "ledger-b")), tDestClock)
	tStore := protocol.NewProtocolStore(filepath.Join(timeoutRoot, "protocol"))
	fatal(tSource.Bootstrap(ctx, protocol.Account{Name: "alice", Balance: 1000}, protocol.Account{Name: "settlement", Balance: 0}))
	fatal(tDest.Bootstrap(ctx, protocol.Account{Name: "settlement", Balance: 1000}, protocol.Account{Name: "bob", Balance: 0}))
	tCoordinator := protocol.NewCoordinator(tSource, tDest, tStore, protocol.NewMutableClock(start))
	timeoutID := "lab-htlc-timeout"
	_, err = tCoordinator.Create(ctx, protocol.CreateTransfer{ID: timeoutID, Sender: "alice", Receiver: "bob", Amount: 80, Secret: "expired-secret", DestDeadline: start.Add(10 * time.Minute), SourceDeadline: start.Add(20 * time.Minute)})
	fatal(err)
	line("准备态断线", "两侧资金仍冻结；ledger-b 最早 10:10 可退，ledger-a 最早 10:20 可退", start, tSourceClock, tDestClock, tCoordinator, tSource, tDest, timeoutID)
}

func line(when, note string, reference time.Time, sourceClock, destClock protocol.Clock, coordinator *protocol.Coordinator, source, dest *protocol.Ledger, id string) {
	observed, err := coordinator.Observe(context.Background(), id)
	fatal(err)
	sourceAccounts, _, err := source.Snapshot(context.Background())
	fatal(err)
	destAccounts, _, err := dest.Snapshot(context.Background())
	fatal(err)
	fmt.Printf("\n[%s wall=%s source=%s destination=%s]\n%s\n", when, reference.Format("15:04:05"), sourceClock.Now().Format("15:04:05"), destClock.Now().Format("15:04:05"), note)
	fmt.Printf("  ledger-a HTLC=%-8s until=%s alice available=%d held=%d\n", observed.Source.Status, observed.Source.LockUntil.Format("15:04:05"), sourceAccounts["alice"].Balance-sourceAccounts["alice"].Held, sourceAccounts["alice"].Held)
	fmt.Printf("  ledger-b HTLC=%-8s until=%s bob=%d settlement held=%d\n", observed.Dest.Status, observed.Dest.LockUntil.Format("15:04:05"), destAccounts["bob"].Balance, destAccounts["settlement"].Held)
}

func fatal(err error) {
	if err != nil {
		panic(err)
	}
}
